
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.spanner;

import com.google.cloud.WaitForOption;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.TimestampBound;
import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.util.MovingAverage;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link io.permazen.kv.KVDatabase} implementation based on
 * <a href="https://cloud.google.com/spanner/">Google Cloud Spanner</a>.
 *
 * <p><b>Configuration</b></p>
 *
 * <p>
 * A {@link SpannerKVDatabase} must be configured with a Spanner instance ID. You can create a Spanner instance
 * using the <a href="https://console.cloud.google.com/spanner/">Google Cloud Console</a> or the {@code gcloud}
 * command line utility.
 *
 * <p>
 * {@linkplain #setSpannerOptions Configure} a {@link SpannerOptions} to override the default project ID associated
 * with your environment. The default database ID ({@value #DEFAULT_DATABASE_ID}) and table name ({@value #DEFAULT_TABLE_NAME})
 * may also be overridden.
 *
 * <p><b>Caching</b></p>
 *
 * <p>
 * Because Spanner has relatively high latency vs. throughput, instances utilize a {@link io.permazen.kv.caching.CachingKVStore}
 * for caching and batch loading read-ahead.
 *
 * <p><b>Consistency Levels</b></p>
 *
 * <p>
 * Transactions may either have strong consistency (the default), or have some amount of staleness.
 * Transactions that are not strongly consistent must be read-only.
 *
 * <p>
 * A {@link TimestampBound} may be passed as an option to {@link #createTransaction(Map) createTransaction()}
 * under the {@value #OPTION_TIMESTAMP_BOUND} key.
 * Transactions that are not strongly consistent must be read-only.
 *
 * <p>
 * In Spring applications, the transaction consistency level may be configured through the Spring
 * {@link io.permazen.spring.PermazenTransactionManager} by (ab)using the transaction isolation level setting,
 * for example, via the {@link org.springframework.transaction.annotation.Transactional &#64;Transactional} annotation's
 * {@link org.springframework.transaction.annotation.Transactional#isolation isolation()} property:
 *
 * <div style="margin-left: 20px;">
 * <table border="1" cellpadding="3" cellspacing="0" summary="Isolation Level Mapping">
 * <tr style="bgcolor:#ccffcc">
 *  <th align="left">Spring isolation level</th>
 *  <th align="left">{@link SpannerKVDatabase} consistency level</th>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#DEFAULT DEFAULT}</td>
 *  <td>Strong consistency</td>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#SERIALIZABLE SERIALIZABLE}</td>
 *  <td>Strong consistency</td>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#READ_COMMITTED READ_COMMITTED}</td>
 *  <td>Exact staleness of ten seconds</td>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#REPEATABLE_READ REPEATABLE_READ}</td>
 *  <td>Exact staleness of three seconds</td>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#READ_UNCOMMITTED READ_UNCOMMITTED}</td>
 *  <td>N/A</td>
 * </tr>
 * </table>
 * </div>
 *
 * <p><b>Key Watches</b></p>
 *
 * <p>
 * {@linkplain io.permazen.kv.KVTransaction#watchKey Key watches} are not supported.
 *
 * @see <a href="https://cloud.google.com/spanner/">Google Cloud Spanner</a>
 */
@ThreadSafe
public class SpannerKVDatabase implements KVDatabase {

    /**
     * Option key for {@link #createTransaction(Map)}. Value should be a {@link TimestampBound} instance.
     */
    public static final String OPTION_TIMESTAMP_BOUND = "TimestampBound";

    /**
     * Default database ID: {@value #DEFAULT_DATABASE_ID}.
     */
    public static final String DEFAULT_DATABASE_ID = "jsimpledb";

    /**
     * Default table name: {@value #DEFAULT_TABLE_NAME}.
     */
    public static final String DEFAULT_TABLE_NAME = "KV";

    /**
     * Default background task thread pool size.
     */
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;

    // Patterns for validation
    private static final String INSTANCE_ID_PATTERN = "[a-z][-_A-Za-z0-9]*[a-z0-9]";
    private static final String DATABASE_ID_PATTERN = "[a-z][-_A-Za-z0-9]*[a-z0-9]";
    private static final String TABLE_NAME_PATTERN = "[A-Za-z][_A-Za-z0-9]*";

    // RTT
    private static final int INITIAL_RTT_ESTIMATE_MILLIS = 50;                          // 50 ms
    private static final double RTT_ESTIMATE_DECAY_FACTOR = 0.025;

    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    @GuardedBy("this")
    private Spanner spanner;
    @GuardedBy("this")
    private DatabaseClient client;
    @GuardedBy("this")
    private ExecutorService executor;

    @GuardedBy("this")
    private SpannerOptions spannerOptions;
    @GuardedBy("this")
    private String instanceId;
    @GuardedBy("this")
    private String databaseId = DEFAULT_DATABASE_ID;
    @GuardedBy("this")
    private String tableName = DEFAULT_TABLE_NAME;
    @GuardedBy("this")
    private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    @GuardedBy("this")
    private MovingAverage rtt;

    /**
     * Configure {@link SpannerOptions}.
     *
     * @param spannerOptions Google spanner configuration options
     * @throws IllegalStateException if this instance is {@linkplain #start already started}
     * @throws IllegalArgumentException if {@code spannerOptions} is null
     */
    public synchronized void setSpannerOptions(SpannerOptions spannerOptions) {
        Preconditions.checkArgument(spannerOptions != null, "null spannerOptions");
        Preconditions.checkState(this.client == null, "already started");
        this.spannerOptions = spannerOptions;
    }

    /**
     * Set Spanner instance ID.
     *
     * <p>
     * Required property.
     *
     * @param instanceId Spanner instance ID
     * @throws IllegalStateException if this instance is {@linkplain #start already started}
     * @throws IllegalArgumentException if {@code instanceId} is null
     */
    public synchronized void setInstanceId(String instanceId) {
        Preconditions.checkArgument(instanceId != null, "null instanceId");
        Preconditions.checkArgument(Pattern.compile(INSTANCE_ID_PATTERN).matcher(instanceId).matches(), "invalid instanceId");
        Preconditions.checkState(this.client == null, "already started");
        this.instanceId = instanceId;
    }

    /**
     * Set Spanner database ID.
     *
     * <p>
     * Default is {@value #DEFAULT_DATABASE_ID}.
     *
     * @param databaseId Spanner instance ID
     * @throws IllegalStateException if this instance is {@linkplain #start already started}
     * @throws IllegalArgumentException if {@code databaseId} is null
     */
    public synchronized void setDatabaseId(String databaseId) {
        Preconditions.checkArgument(databaseId != null, "null databaseId");
        Preconditions.checkArgument(Pattern.compile(DATABASE_ID_PATTERN).matcher(databaseId).matches(), "invalid databaseId");
        Preconditions.checkState(this.client == null, "already started");
        this.databaseId = databaseId;
    }

    /**
     * Set Spanner table name.
     *
     * <p>
     * Default is {@value #DEFAULT_TABLE_NAME}.
     *
     * @param tableName Spanner database table name
     * @throws IllegalStateException if this instance is {@linkplain #start already started}
     * @throws IllegalArgumentException if {@code tableName} is invalid
     * @throws IllegalArgumentException if {@code tableName} is null
     */
    public synchronized void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null, "null tableName");
        Preconditions.checkArgument(Pattern.compile(TABLE_NAME_PATTERN).matcher(tableName).matches(), "invalid tableName");
        Preconditions.checkState(this.client == null, "already started");
        this.tableName = tableName;
    }

// Lifecycle

    /**
     * Start this instance.
     *
     * <p>
     * The configured Spanner instance, database, and table will be created automatically as needed.
     */
    @Override
    @PostConstruct
    public synchronized void start() {

        // Sanity check
        if (this.spanner != null)
            return;
        Preconditions.checkState(this.instanceId != null, "no instance ID configured");

        // Use default options if none provided
        if (this.spannerOptions == null)
            this.spannerOptions = SpannerOptions.newBuilder().build();

        // Create Spanner access object
        this.spanner = this.spannerOptions.getService();
        boolean success = false;
        try {

            // Setup instance if needed
            final Instance instance = this.setupInstance(this.spanner.getInstanceAdminClient());

            // Setup database if needed
            final Database database = this.setupDatabase(instance);

            // Setup database table
            this.setupTable(database);

            // Get client
            final DatabaseId did = DatabaseId.of(instance.getId(), this.databaseId);
            this.client = this.spanner.getDatabaseClient(did);

            // Create thread pool
            this.executor = Executors.newFixedThreadPool(this.threadPoolSize, r -> {
                final Thread thread = new Thread(r);
                thread.setName(this.getClass().getSimpleName() + "-" + THREAD_COUNTER.incrementAndGet());
                return thread;
            });

            // Initialize RTT
            this.rtt = new MovingAverage(RTT_ESTIMATE_DECAY_FACTOR, INITIAL_RTT_ESTIMATE_MILLIS);

            // Done
            success = true;
        } finally {
            if (!success)
                this.cleanup();
        }
    }

    @Override
    @PreDestroy
    public synchronized void stop() {

        // Already stopped?
        if (this.spanner == null)
            return;

        // Cleanup
        this.cleanup();
    }

    private synchronized void cleanup() {
        if (this.spanner != null) {
            this.spanner.closeAsync();
            this.spanner = null;
        }
        if (this.executor != null) {
            this.executor.shutdownNow();
            try {
                this.executor.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            this.executor = null;
        }
        this.client = null;
    }

// Accessors

    /**
     * Get the number of threads in the background task thread pool.
     *
     * <p>
     * Default value is {@value #DEFAULT_THREAD_POOL_SIZE}.
     *
     * @return number of threads in thread pool
     */
    public synchronized int getThreadPoolSize() {
        return this.threadPoolSize;
    }

    /**
     * Set the number of threads in the background task thread pool.
     *
     * <p>
     * Default value is {@value #DEFAULT_THREAD_POOL_SIZE}.
     *
     * @param threadPoolSize number of threads in thread pool
     * @throws IllegalStateException if this instance is already started
     * @throws IllegalArgumentException if {@code threadPoolSize <= 0}
     */
    public synchronized void setThreadPoolSize(int threadPoolSize) {
        Preconditions.checkArgument(threadPoolSize > 0, "threadPoolSize <= 0");
        Preconditions.checkState(this.spanner == null, "already started");
        this.threadPoolSize = threadPoolSize;
    }

// RTT estimate

    /**
     * Get the current round trip time estimate.
     *
     * @return current RTT estimate in nanoseconds
     * @throws IllegalStateException if this instance has never {@link #start}ed
     */
    public synchronized double getRttEstimate() {
        Preconditions.checkState(this.rtt != null, "instance has never started");
        return this.rtt.get();
    }

    synchronized void updateRttEstimate(double rtt) {
        this.rtt.add(rtt);
    }

// Setup

    private Instance setupInstance(InstanceAdminClient instanceAdminClient) {
        this.log.debug("finding spanner instance with ID \"" + this.instanceId + "\"");
        final Instance instance = instanceAdminClient.getInstance(this.instanceId);
        this.log.debug("found spanner instance with ID \"" + this.instanceId + "\"");
        return instance;
    }

    private Database setupDatabase(Instance instance) {

        // Does database already exist?
        this.log.debug("finding spanner database with ID \"" + this.databaseId + "\"");
        try {
            final Database database = instance.getDatabase(this.databaseId);
            this.log.debug("found spanner database with ID \"" + this.databaseId + "\"");
            return database;
        } catch (SpannerException e) {
            if (!ErrorCode.NOT_FOUND.equals(e.getErrorCode()))
                throw e;
            this.log.debug("spanner database with ID \"" + this.databaseId + "\" not found");
        }

        // Create new database
        this.log.info("creating new spanner database with ID \"" + this.instanceId + "\"");
        return this.waitFor(instance.createDatabase(this.databaseId, Collections.singleton(this.getCreateTableDDL())));
    }

    private void setupTable(Database database) {

        // Does table already exist?
        this.log.debug("finding key/value database table with name \"" + this.tableName + "\"");
        final String expectedDDL = this.normalizeDDL(this.getCreateTableDDL());
        for (String statement : database.getDdl()) {
            if (this.normalizeDDL(statement).equals(expectedDDL)) {
                this.log.debug("found key/value database table with name \"" + this.tableName + "\"");
                return;
            }
        }
        this.log.debug("key/value database table with name \"" + this.tableName + "\" not found");

        // Create new table
        final String ddl = this.getCreateTableDDL();
        this.log.info("creating new key/value database table with name \"" + this.tableName + "\":\n" + ddl);
        this.waitFor(database.updateDdl(Collections.singleton(ddl), null));
    }

    private String getCreateTableDDL() {
        return "CREATE TABLE " + this.tableName + " (\n"
          + "  key BYTES(MAX) NOT NULL,\n"
          + "  val BYTES(MAX) NOT NULL,\n"
          + ") PRIMARY KEY(key)";
    }

    private String normalizeDDL(String ddl) {
        return ddl.trim()
          .replaceAll("([^-_A-Za-z0-9])\\s+([^-_A-Za-z0-9])", "$1$2")
          .replaceAll("\\s+", " ")
          .toLowerCase();
    }

    private <T> T waitFor(Operation<T, ?> operation) {
        return operation.waitFor(WaitForOption.checkEvery(500, TimeUnit.MILLISECONDS)).getResult();
    }

// Transactions

    @Override
    public SpannerKVTransaction createTransaction() {
        return this.createTransaction((Map<String, ?>)null);
    }

    @Override
    public SpannerKVTransaction createTransaction(Map<String, ?> options) {

        // Get default consistency
        TimestampBound consistency = TimestampBound.strong();

        // Any options?
        if (options != null) {

            // Look for options from the PermazenTransactionManager
            Object isolation = options.get("org.springframework.transaction.annotation.Isolation");
            if (isolation instanceof Enum)
                isolation = ((Enum<?>)isolation).name();
            if (isolation != null) {
                switch (isolation.toString()) {
                case "READ_COMMITTED":
                    consistency = TimestampBound.ofExactStaleness(10, TimeUnit.SECONDS);
                    break;
                case "REPEATABLE_READ":
                    consistency = TimestampBound.ofExactStaleness(3, TimeUnit.SECONDS);
                    break;
                case "SERIALIZABLE":
                    consistency = TimestampBound.strong();
                    break;
                default:
                    break;
                }
            }

            // Look for OPTION_TIMESTAMP_BOUND option
            try {
                final Object value = options.get(OPTION_TIMESTAMP_BOUND);
                if (value instanceof TimestampBound)
                    consistency = (TimestampBound)value;
            } catch (Exception e) {
                // ignore
            }
        }

        // Configure consistency level
        return this.createTransaction(consistency);
    }

    protected synchronized SpannerKVTransaction createTransaction(TimestampBound consistency) {

        // Sanity check
        Preconditions.checkState(this.spanner != null, "instance is not started");

        // Create transaction
        return new SpannerKVTransaction(this, this.client, this.tableName, consistency);
    }

    protected synchronized ExecutorService getExecutorService() {
        return this.executor;
    }

// Snapshots

    /**
     * Create a read-only snapshot of the database with the given timestamp bound.
     *
     * @param consistency consistency for the snapshot
     * @return read-only view of database
     * @throws IllegalArgumentException if {@code consistency} is null
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    public synchronized ReadOnlySpannerView snapshot(TimestampBound consistency) {
        Preconditions.checkState(this.spanner != null, "instance is not started");
        return new ReadOnlySpannerView(this.tableName, this.client.readOnlyTransaction(consistency));
    }
}

