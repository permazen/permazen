
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Bytes;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.dellroad.stuff.io.ByteBufferInputStream;
import org.dellroad.stuff.io.ByteBufferOutputStream;
import org.dellroad.stuff.java.TimedWait;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.leveldb.LevelDBKVStore;
import org.jsimpledb.kv.mvcc.AtomicKVStore;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.mvcc.Mutations;
import org.jsimpledb.kv.mvcc.Reads;
import org.jsimpledb.kv.mvcc.Writes;
import org.jsimpledb.kv.raft.msg.AppendRequest;
import org.jsimpledb.kv.raft.msg.AppendResponse;
import org.jsimpledb.kv.raft.msg.CommitRequest;
import org.jsimpledb.kv.raft.msg.CommitResponse;
import org.jsimpledb.kv.raft.msg.GrantVote;
import org.jsimpledb.kv.raft.msg.InstallSnapshot;
import org.jsimpledb.kv.raft.msg.Message;
import org.jsimpledb.kv.raft.msg.MessageSwitch;
import org.jsimpledb.kv.raft.msg.RequestVote;
import org.jsimpledb.kv.raft.net.Network;
import org.jsimpledb.kv.raft.net.TCPNetwork;
import org.jsimpledb.kv.util.PrefixKVStore;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.LongEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A distributed {@link org.jsimpledb.kv.KVDatabase} based on the Raft consensus algorithm.
 *
 * <p>
 * Raft defines a distributed consensus algorithm for maintaining a shared state machine.
 * Each Raft node maintains a complete copy of the state machine. Cluster nodes elect a
 * leader who collects and distributes updates and provides for consistent reads.
 * As long as as a node is part of a majority, the state machine is fully operational.
 *
 * <p>
 * {@link RaftKVDatabase} turns this into a transactional key/value database with linearizable ACID semantics as follows:
 *  <ul>
 *  <li>The Raft state machine is the key/value store data.</li>
 *  <li>Unapplied log entries are stored on disk as serialized mutations, and also cached in memory.</li>
 *  <li>On leaders only, committed log entries are not applied immediately; instead they kept around for time at least
 *      <i>T<sub>max</sub></i> since creation, where <i>T<sub>max</sub></i> is the maximum supported transaction duration.
 *      This caching is subject to <i>M<sub>max</sub></i>, which limits the total memory used; if reached, these log entries
 *      are applied early.</li>
 *  <li>Concurrent transactions are supported through a simple optimistic locking MVCC scheme (same as used by
 *      {@link org.jsimpledb.kv.mvcc.SnapshotKVDatabase}):
 *      <ul>
 *      <li>Transactions execute locally until commit time, using a {@link MutableView} to collect mutations.
 *          The {@link MutableView} is based on the local node's most recent log entry (whether committed or not);
 *          this is called the <i>base term and index</i> for the transaction.</li>
 *      <li>On commit, the transaction's {@link Reads}, {@link Writes}, base index and term, and any config change are
 *          {@linkplain CommitRequest sent} to the leader.</li>
 *      <li>The leader confirms that the log entry corresponding to the transaction's base index is either not yet applied,
 *          or was its most recently applied log entry. If this is not the case, then the transaction's base log entry
 *          is too old (older than <i>T<sub>max</sub></i>, or was applied and discarded early due to memory pressure),
 *          and so the transaction is rejected with a {@link RetryTransactionException}.
 *      <li>The leader verifies that the the log entry term matches the transaction's base term; if not, the base log entry
 *          has been overwritten, and the transaction is rejected with a {@link RetryTransactionException}.
 *      <li>The leader confirms that the {@link Writes} associated with log entries after the transaction's base log entry
 *          do not create {@linkplain Reads#isConflict conflicts} when compared against the transaction's {@link Reads}.
 *          If so, the transaction is rejected with a {@link RetryTransactionException}.</li>
 *      <li>The leader adds a new log entry consisting of the transaction's {@link Writes} (and any config change) to its log.
 *          The associated term and index become the transaction's <i>commit term and index</i>; the leader then
 *          {@linkplain CommitResponse replies} to the follower with this information.</li>
 *      <li>If/when the follower sees a <i>committed</i> (in the Raft sense) log entry appear in its log matching the
 *          transaction's commit term and index, then the transaction is complete.</li>
 *      <li>As an optimization, when the leader sends a log entry to the same follower who committed the corresponding
 *          transaction in the first place, only the transaction ID is sent, because the follower already has the data.</li>
 *      </ul>
 *  </li>
 *  <li>For transactions occurring on a leader, the logic is similar except of course no network communication occurs.</li>
 *  <li>For read-only transactions, the leader does not create a new log entry; instead, the transaction's commit
 *      term and index are set to the base term and index, and the leader also calculates its current "leader lease timeout",
 *      which is the earliest time at which it is possible for another leader to be elected.
 *      This is calculated as the time in the past at which the leader sent {@link AppendRequest}'s to a majority of followers
 *      who have since responded, plus the {@linkplain #setMinElectionTimeout minimum election timeout}, minus a small adjustment
 *      for possible clock drift (this assumes all nodes have the same minimum election timeout configured). If the current
 *      time is prior to the leader lease timeout, the transaction may be committed as soon as log entry corresponding to the
 *      commit term and index is committed (it may already be); otherwise, the current time is returned to the follower
 *      as minimum required leader lease timeout before the transaction may be committed.</li>
 *  <li>Every {@link AppendRequest} includes the leader's current timestamp and leader lease timeout, so followers can commit
 *      any waiting read-only transactions. Leaders keep track of which followers are waiting on which leader lease
 *      timeout values, and when the leader lease timeout advances to allow a follower to commit a transaction, the follower
 *      is immediately notified.</li>
 *  <li>A weaker consistency guarantee for read-only transactions (stale reads) is also possible; see
 *      {@link RaftKVTransaction#setReadOnlySnapshot RaftKVTransaction.setReadOnlySnapshot()}. Typically these
 *      transactions will generate no network traffic.</li>
 *  </ul>
 *
 * <p><b>Limitations</b></p>
 *
 * <ul>
 *  <li>A transaction's mutations must fit in memory.</li>
 *  <li>An {@link AtomicKVStore} is required to store local persistent state;
 *      if none is configured, a {@link LevelDBKVStore} is used.</li>
 *  <li>All nodes must be configured with the same {@linkplain #setMinElectionTimeout minimum election timeout}.
 *      This guarantees that the leader's lease timeout calculation is valid.</li>
 *  <li>Due to the optimistic locking approach used, this implementation will perform poorly when there is a high
 *      rate of conflicting transactions; the result will be many transaction retries.</li>
 *  <li>Performance will suffer when the amount of data associated with a typical transaction cannot be delivered
 *      quickly and reliably over the network.</li>
 * </ul>
 *
 * <p>
 * In general, the algorithm should function correctly under all non-Byzantine conditions. The level of difficultly
 * the system is experiencing, due to contention, network errors, etc., can be measured in terms of:
 * <ul>
 *  <li>The average amount of time it takes to commit a transaction</li>
 *  <li>The frequency of {@link RetryTransactionException}'s</li>
 * </ul>
 *
 * <p><b>Cluster Configuration</b></p>
 *
 * <p>
 * Instances support dynamic cluster configuration changes at runtime.
 *
 * <p>
 * Initially, all nodes are in an <i>unconfigured</i> state, where nothing has been added to the Raft log yet and no
 * cluster is defined. Unconfigured nodes are passive: they stay in follower mode (i.e., they will not start elections),
 * and they disallow local transactions that make any changes other than as described below to initialize a new cluster.
 *
 * <p>
 * An unconfigured node becomes configured when either:
 * <ol>
 *  <li>{@link RaftKVTransaction#configChange RaftKVTransaction.configChange()} is invoked and committed within
 *      a local transaction, which creates a new single node cluster and commits the first log entry; or</li>
 *  <li>An {@link AppendRequest} is received from a leader of some existing cluster, in which case the node
 *      records the cluster ID (see below) and applies the received cluster configuration</li>
 * </ol>
 *
 * <p>
 * A node is configured if and only if it has recorded one or more log entries. The very first log entry
 * always contains the initial cluster configuration (containing only the node that created it), so any node that has a
 * non-empty log is configured.
 *
 * <p>
 * Newly created clusters are assigned a random 32-bit cluster ID (option #1 above). This ID is included in all messages sent
 * over the network, and adopted by unconfigured nodes that join the cluster (via option #2 above). Nodes discard incoming
 * messages containing a cluster ID different from one they have seen previously. This prevents data corruption that can occur
 * if nodes from two different clusters are inadvertently "mixed" together.
 *
 * <p>
 * Once a node joins a cluster with a specific cluster ID, it cannot be reassigned to a different cluster without first
 * returning it to the unconfigured state; to do that, it must be shut it down and its persistent state deleted.
 *
 * <p><b>Configuration Changes</b></p>
 *
 * <p>
 * Once a node configured, a separate issue is whether the node is <i>included</i> in its own configuration, i.e., whether
 * the node is a member of its cluster. A node that is not a member of its cluster does not count its own vote to determine
 * committed log entries (if a leader), and does not start elections (if a follower). However, it will accept and respond
 * to incoming {@link AppendRequest}s and {@link RequestVote}s.
 *
 * <p>
 * In addition, leaders follow these rules:
 * <ul>
 *  <li>If a leader is removed from a cluster, it remains the leader until the configuration change that
 *      removed it is committed (not counting its own vote), and then steps down (reverts to follower).</li>
 *  <li>If a follower is added to a cluster, the leader immediately starts sending that follower {@link AppendRequest}s.</li>
 *  <li>If a follower is removed from a cluster, the leader continues to send that follower {@link AppendRequest}s
 *      until the follower acknowledges receipt of the log entry containing the configuration change.</li>
 *  <li>Configuration changes that remove the last node in a cluster are disallowed.</li>
 *  <li>Only one configuration change may take place at a time.</li>
 * </ul>
 *
 * @see <a href="https://raftconsensus.github.io/">The Raft Consensus Algorithm</a>
 */
public class RaftKVDatabase implements KVDatabase {

    /**
     * Default minimum election timeout ({@value #DEFAULT_MIN_ELECTION_TIMEOUT}ms).
     *
     * @see #setMinElectionTimeout
     */
    public static final int DEFAULT_MIN_ELECTION_TIMEOUT = 750;

    /**
     * Default maximum election timeout ({@value #DEFAULT_MAX_ELECTION_TIMEOUT}ms).
     *
     * @see #setMaxElectionTimeout
     */
    public static final int DEFAULT_MAX_ELECTION_TIMEOUT = 1000;

    /**
     * Default heartbeat timeout ({@value DEFAULT_HEARTBEAT_TIMEOUT}ms).
     *
     * @see #setHeartbeatTimeout
     */
    public static final int DEFAULT_HEARTBEAT_TIMEOUT = 200;

    /**
     * Default maximum supported outstanding transaction duration ({@value DEFAULT_MAX_TRANSACTION_DURATION}ms).
     *
     * @see #setMaxTransactionDuration
     */
    public static final int DEFAULT_MAX_TRANSACTION_DURATION = 5 * 1000;

    /**
     * Default maximum supported applied log entry memory usage ({@value DEFAULT_MAX_APPLIED_LOG_MEMORY} bytes).
     *
     * @see #setMaxAppliedLogMemory
     */
    public static final long DEFAULT_MAX_APPLIED_LOG_MEMORY = 100 * 1024 * 1024;                // 100MB

    /**
     * Default transaction commit timeout ({@value DEFAULT_COMMIT_TIMEOUT}).
     *
     * @see #setCommitTimeout
     * @see RaftKVTransaction#setTimeout
     */
    public static final int DEFAULT_COMMIT_TIMEOUT = 5000;                                      // 5 seconds

    // Internal constants
    private static final int MAX_SNAPSHOT_TRANSMIT_AGE = (int)TimeUnit.SECONDS.toMillis(90);    // 90 seconds
    private static final int MAX_SLOW_FOLLOWER_APPLY_DELAY_HEARTBEATS = 10;
    private static final int MAX_UNAPPLIED_LOG_ENTRIES = 64;
    private static final int FOLLOWER_LINGER_HEARTBEATS = 3;                // how long to keep updating removed followers
    private static final float MAX_CLOCK_DRIFT = 0.01f;                     // max clock drift (ratio) in one min election timeout

    // File prefixes and suffixes
    private static final String TX_FILE_PREFIX = "tx-";
    private static final String TEMP_FILE_PREFIX = "temp-";
    private static final String TEMP_FILE_SUFFIX = ".tmp";
    private static final String KVSTORE_FILE_SUFFIX = ".kvstore";
    private static final Pattern TEMP_FILE_PATTERN = Pattern.compile(".*" + Pattern.quote(TEMP_FILE_SUFFIX));
    private static final Pattern KVSTORE_FILE_PATTERN = Pattern.compile(".*" + Pattern.quote(KVSTORE_FILE_SUFFIX));

    // Keys for persistent Raft state
    private static final byte[] CLUSTER_ID_KEY = ByteUtil.parse("0001");
    private static final byte[] CURRENT_TERM_KEY = ByteUtil.parse("0002");
    private static final byte[] LAST_APPLIED_TERM_KEY = ByteUtil.parse("0003");
    private static final byte[] LAST_APPLIED_INDEX_KEY = ByteUtil.parse("0004");
    private static final byte[] LAST_APPLIED_CONFIG_KEY = ByteUtil.parse("0005");
    private static final byte[] VOTED_FOR_KEY = ByteUtil.parse("0006");

    // Prefix for all state machine key/value keys
    private static final byte[] STATE_MACHINE_PREFIX = ByteUtil.parse("80");

    // Logging
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    // Configuration state
    private Network network = new TCPNetwork();
    private String identity;
    private AtomicKVStore kvstore;
    private int minElectionTimeout = DEFAULT_MIN_ELECTION_TIMEOUT;
    private int maxElectionTimeout = DEFAULT_MAX_ELECTION_TIMEOUT;
    private int heartbeatTimeout = DEFAULT_HEARTBEAT_TIMEOUT;
    private int maxTransactionDuration = DEFAULT_MAX_TRANSACTION_DURATION;
    private int commitTimeout = DEFAULT_COMMIT_TIMEOUT;
    private long maxAppliedLogMemory = DEFAULT_MAX_APPLIED_LOG_MEMORY;
    private File logDir;

    // Raft runtime state
    private Role role;                                                  // Raft state: LEADER, FOLLOWER, or CANDIDATE
    private SecureRandom random;                                        // used to randomize election timeout, etc.
    private int clusterId;                                              // cluster ID (zero if unconfigured - usually)
    private long currentTerm;                                           // current Raft term (zero if unconfigured)
    private long commitIndex;                                           // current Raft commit index (zero if unconfigured)
    private long lastAppliedTerm;                                       // key/value store last applied term (zero if unconfigured)
    private long lastAppliedIndex;                                      // key/value store last applied index (zero if unconfigured)
    private final ArrayList<LogEntry> raftLog = new ArrayList<>();      // unapplied log entries (empty if unconfigured)
    private Map<String, String> lastAppliedConfig;                      // key/value store last applied config (empty if none)
    private Map<String, String> currentConfig;                          // most recent cluster config (empty if unconfigured)

    // Non-Raft runtime state
    private AtomicKVStore kv;
    private FileChannel logDirChannel;
    private ScheduledExecutorService executor;
    private String returnAddress;                                       // return address for message currently being processed
    private final HashSet<String> transmitting = new HashSet<>();       // network addresses whose output queues are not empty
    private final HashMap<Long, RaftKVTransaction> openTransactions = new HashMap<>();
    private final LinkedHashSet<Service> pendingService = new LinkedHashSet<>();
    private boolean performingService;
    private boolean shuttingDown;                                       // prevents new transactions from being created

// Configuration

    /**
     * Configure the {@link AtomicKVStore} in which local persistent state is stored.
     *
     * <p>
     * If this property is left unconfigured, by default a {@link LevelDBKVStore} is created when
     * {@link #start} is invoked and shutdown on {@link #stop}.
     *
     * @param kvstore local persistent data store
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setKVStore(AtomicKVStore kvstore) {
        Preconditions.checkState(this.role == null, "already started");
        this.kvstore = kvstore;
    }

    /**
     * Configure the directory in which uncommitted log entries are stored.
     *
     * <p>
     * Required property.
     *
     * @param directory log directory
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setLogDirectory(File directory) {
        Preconditions.checkState(this.role == null, "already started");
        this.logDir = directory;
    }

    /**
     * Configure the {@link Network} to use for inter-node communication.
     *
     * <p>
     * By default, a {@link TCPNetwork} instance is used.
     *
     * @param network network implementation; must not be {@linkplain Network#start started}
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setNetwork(Network network) {
        Preconditions.checkState(this.role == null, "already started");
        this.network = network;
    }

    /**
     * Configure the Raft identity.
     *
     * <p>
     * Required property unless this is a single-node cluster.
     *
     * @param identity unique Raft identity of this node in its cluster
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setIdentity(String identity) {
        Preconditions.checkState(this.role == null, "already started");
        this.identity = identity;
    }

    /**
     * Get this node's Raft identity.
     *
     * @return the unique identity of this node in its cluster
     */
    public synchronized String getIdentity() {
        return this.identity;
    }

    /**
     * Configure the minimum election timeout.
     *
     * <p>
     * This must be set to a value greater than the {@linkplain #setHeartbeatTimeout heartbeat timeout}.
     *
     * <p>
     * Default is {@link #DEFAULT_MIN_ELECTION_TIMEOUT}.
     *
     * <p>
     * <b>Warning:</b> currently all nodes must have the same configured minimum election timeout,
     * otherwise read-only transactions are not guaranteed to be completely up-to-date.
     *
     * @param timeout minimum election timeout in milliseconds
     * @throws IllegalStateException if this instance is already started
     * @throws IllegalArgumentException if {@code timeout <= 0}
     */
    public synchronized void setMinElectionTimeout(int timeout) {
        Preconditions.checkArgument(timeout > 0, "timeout <= 0");
        Preconditions.checkState(this.role == null, "already started");
        this.minElectionTimeout = timeout;
    }

    /**
     * Configure the maximum election timeout.
     *
     * <p>
     * Default is {@link #DEFAULT_MAX_ELECTION_TIMEOUT}.
     *
     * @param timeout maximum election timeout in milliseconds
     * @throws IllegalStateException if this instance is already started
     * @throws IllegalArgumentException if {@code timeout <= 0}
     */
    public synchronized void setMaxElectionTimeout(int timeout) {
        Preconditions.checkArgument(timeout > 0, "timeout <= 0");
        Preconditions.checkState(this.role == null, "already started");
        this.maxElectionTimeout = timeout;
    }

    /**
     * Configure the heartbeat timeout.
     *
     * <p>
     * This must be set to a value less than the {@linkplain #setMinElectionTimeout minimum election timeout}.
     *
     * <p>
     * Default is {@link #DEFAULT_HEARTBEAT_TIMEOUT}.
     *
     * @param timeout heartbeat timeout in milliseconds
     * @throws IllegalStateException if this instance is already started
     * @throws IllegalArgumentException if {@code timeout <= 0}
     */
    public synchronized void setHeartbeatTimeout(int timeout) {
        Preconditions.checkArgument(timeout > 0, "timeout <= 0");
        Preconditions.checkState(this.role == null, "already started");
        this.heartbeatTimeout = timeout;
    }

    /**
     * Configure the maximum supported duration for outstanding transactions.
     *
     * <p>
     * This value is the <i>T<sub>max</sub></i> value from the {@linkplain RaftKVDatabase overview}.
     * A larger value means more memory may be used.
     *
     * <p>
     * This value may be changed while this instance is already running.
     *
     * <p>
     * Default is {@link #DEFAULT_MAX_TRANSACTION_DURATION}.
     *
     * @param duration maximum supported duration for outstanding transactions in milliseconds
     * @throws IllegalArgumentException if {@code duration <= 0}
     * @see #setMaxAppliedLogMemory
     */
    public synchronized void setMaxTransactionDuration(int duration) {
        Preconditions.checkArgument(duration > 0, "duration <= 0");
        this.maxTransactionDuration = duration;
    }

    /**
     * Configure the maximum allowed memory used for caching log entries that have already been applied to the state machine.
     *
     * <p>
     * This value is the <i>M<sub>max</sub></i> value from the {@linkplain RaftKVDatabase overview}.
     * A higher value means transactions may be larger and/or stay open longer without causing a {@link RetryTransactionException}.
     *
     * <p>
     * This value is approximate, and only affects leaders; followers do not cache applied log entries.
     *
     * <p>
     * This value may be changed while this instance is already running.
     *
     * <p>
     * Default is {@link #DEFAULT_MAX_APPLIED_LOG_MEMORY}.
     *
     * @param memory maximum allowed memory usage for cached applied log entries
     * @throws IllegalArgumentException if {@code memory <= 0}
     * @see #setMaxTransactionDuration
     */
    public synchronized void setMaxAppliedLogMemory(long memory) {
        Preconditions.checkArgument(memory > 0, "memory <= 0");
        this.maxAppliedLogMemory = memory;
    }

    /**
     * Configure the default transaction commit timeout.
     *
     * <p>
     * This value determines how transactions will wait once {@link RaftKVTransaction#commit commit()}
     * is invoked for the commit to succeed. This can be overridden on a per-transaction basis via
     * {@link RaftKVTransaction#setTimeout}.
     *
     * <p>
     * This value may be changed while this instance is already running.
     *
     * <p>
     * Default is {@link #DEFAULT_COMMIT_TIMEOUT}.
     *
     * @param timeout transaction commit timeout in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code timeout} is negative
     * @see RaftKVTransaction#setTimeout
     */
    public synchronized void setCommitTimeout(int timeout) {
        Preconditions.checkArgument(timeout >= 0, "timeout < 0");
        this.commitTimeout = commitTimeout;
    }

// Lifecycle

    /**
     * Start this instance.
     *
     * <p>
     * Does nothing if already started or in the process of shutting down.
     * </p>
     *
     * @throws IllegalStateException if this instance is not properly configured
     * @throws IllegalArgumentException if persistent data is corrupted
     * @throws IOException if an I/O error occurs
     */
    @PostConstruct
    public synchronized void start() throws IOException {

        // Sanity check
        assert this.checkState();
        if (this.role != null)
            return;
        Preconditions.checkState(!this.shuttingDown, "shutdown in progress");
        Preconditions.checkState(this.logDir != null, "no log directory configured");
        Preconditions.checkState(this.network != null, "no network configured");
        Preconditions.checkState(this.kv == null, "key/value store exists");
        Preconditions.checkState(this.minElectionTimeout <= this.maxElectionTimeout, "minElectionTimeout > maxElectionTimeout");
        Preconditions.checkState(this.heartbeatTimeout < this.minElectionTimeout, "heartbeatTimeout >= minElectionTimeout");
        Preconditions.checkState(this.identity != null, "no identity configured");

        // Log
        this.info("starting " + this + " in directory " + this.logDir);

        // Start up local database
        boolean success = false;
        try {

            // Create/verify log directory
            if (!this.logDir.exists() && !this.logDir.isDirectory())
                throw new IOException("failed to create directory `" + this.logDir + "'");
            if (!this.logDir.isDirectory())
                throw new IOException("file `" + this.logDir + "' is not a directory");

            // By default use an atomic key/value store based on LevelDB if not explicitly specified
            if (this.kvstore != null)
                this.kv = this.kvstore;
            else {
                final File leveldbDir = new File(this.logDir, "levedb" + KVSTORE_FILE_SUFFIX);
                if (!leveldbDir.exists() && !leveldbDir.mkdirs())
                    throw new IOException("failed to create directory `" + leveldbDir + "'");
                if (!leveldbDir.isDirectory())
                    throw new IOException("file `" + leveldbDir + "' is not a directory");
                this.kv = new LevelDBKVStore(new Iq80DBFactory().open(leveldbDir, new Options().createIfMissing(true)));
            }

            // Open directory containing log entry files so we have a way to fsync() it
            assert this.logDirChannel == null;
            this.logDirChannel = FileChannel.open(this.logDir.toPath(), StandardOpenOption.READ);

            // Create randomizer
            assert this.random == null;
            this.random = new SecureRandom();

            // Start up executor thread
            assert this.executor == null;
            this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable action) {
                    return new ServiceThread(action);
                }
            });

            // Start network
            this.network.start(new Network.Handler() {
                @Override
                public void handle(String sender, ByteBuffer buf) {
                    RaftKVDatabase.this.handle(sender, buf);
                }

                @Override
                public void outputQueueEmpty(String address) {
                    RaftKVDatabase.this.outputQueueEmpty(address);
                }
            });

            // Reload persistent raft info
            this.clusterId = (int)this.decodeLong(CLUSTER_ID_KEY, 0);
            this.currentTerm = this.decodeLong(CURRENT_TERM_KEY, 0);
            final String votedFor = this.decodeString(VOTED_FOR_KEY, null);
            this.lastAppliedTerm = this.decodeLong(LAST_APPLIED_TERM_KEY, 0);
            this.lastAppliedIndex = this.decodeLong(LAST_APPLIED_INDEX_KEY, 0);
            this.lastAppliedConfig = this.decodeConfig(LAST_APPLIED_CONFIG_KEY);
            this.currentConfig = this.buildCurrentConfig();

            // If we crashed part way through a snapshot install, recover by resetting state machine
            if (this.lastAppliedTerm < 0 || this.lastAppliedIndex < 0) {
                this.info("detected partially applied snapshot, resetting state machine");
                if (!this.resetStateMachine(true))
                    throw new IOException("error resetting state machine");
            }

            // Initialize commit index
            this.commitIndex = this.lastAppliedIndex;

            // Reload outstanding log entries from disk
            this.loadLog();

            // Show recovered state
            this.info("recovered Raft state:"
              + "\n  clusterId=" + (this.clusterId != 0 ? String.format("0x%08x", this.clusterId) : "none")
              + "\n  currentTerm=" + this.currentTerm
              + "\n  lastApplied=" + this.lastAppliedIndex + "t" + this.lastAppliedTerm
              + "\n  lastAppliedConfig=" + this.lastAppliedConfig
              + "\n  currentConfig=" + this.currentConfig
              + "\n  votedFor=" + (votedFor != null ? "\"" + votedFor + "\"" : "nobody")
              + "\n  log=" + this.raftLog);

            // Validate recovered state
            if (this.isConfigured()) {
                Preconditions.checkArgument(this.clusterId != 0);
                Preconditions.checkArgument(this.currentTerm > 0);
                Preconditions.checkArgument(this.getLastLogTerm() > 0);
                Preconditions.checkArgument(this.getLastLogIndex() > 0);
                Preconditions.checkArgument(!this.currentConfig.isEmpty());
            } else {
                Preconditions.checkArgument(this.lastAppliedTerm == 0);
                Preconditions.checkArgument(this.lastAppliedIndex == 0);
                Preconditions.checkArgument(this.currentTerm == 0);
                Preconditions.checkArgument(this.getLastLogTerm() == 0);
                Preconditions.checkArgument(this.getLastLogIndex() == 0);
                Preconditions.checkArgument(this.currentConfig.isEmpty());
                Preconditions.checkArgument(this.raftLog.isEmpty());
            }

            // Start as follower (with unknown leader)
            this.changeRole(new FollowerRole(this, null, null, votedFor));

            // Done
            success = true;
        } finally {
            if (!success)
                this.cleanup();
        }

        // Sanity check
        assert this.checkState();
    }

    /**
     * Stop this instance.
     *
     * <p>
     * Does nothing if not {@linkplain #start started} or in the process of shutting down.
     * </p>
     */
    @PreDestroy
    public void stop() {

        // Set flag to prevent new transactions
        synchronized (this) {

            // Sanity check
            assert this.checkState();
            if (this.role == null || this.shuttingDown)
                return;

            // Set shutting down flag
            this.info("starting shutdown of " + this);
            this.shuttingDown = true;

            // Fail all remaining open transactions
            for (RaftKVTransaction tx : new ArrayList<RaftKVTransaction>(this.openTransactions.values())) {
                switch (tx.getState()) {
                case EXECUTING:
                    tx.rollback();
                    break;
                case COMMIT_READY:
                case COMMIT_WAITING:
                    this.fail(tx, new KVTransactionException(tx, "database shutdown"));
                    break;
                case COMPLETED:
                    break;
                default:
                    assert false;
                    break;
                }
            }

            // Sleep while we wait for transactions to clean themselves up
            boolean done = false;
            try {
                done = TimedWait.wait(this, 5000, new org.dellroad.stuff.java.Predicate() {
                    @Override
                    public boolean test() {
                        return RaftKVDatabase.this.openTransactions.isEmpty();
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!done)
                this.warn("open transactions not cleaned up during shutdown");
        }

        // Shut down the executor and wait for pending tasks to finish
        this.executor.shutdownNow();
        try {
            this.executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Final cleanup
        synchronized (this) {
            this.executor = null;
            this.cleanup();
        }

        // Done
        this.info("completed shutdown of " + this);
    }

    private void cleanup() {
        assert Thread.holdsLock(this);
        assert this.openTransactions.isEmpty();
        if (this.role != null) {
            this.role.shutdown();
            this.role = null;
        }
        if (this.executor != null) {
            this.executor.shutdownNow();
            try {
                this.executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            this.executor = null;
        }
        if (this.kv != null) {
            if (this.kvstore == null) {
                final LevelDBKVStore ldb = (LevelDBKVStore)this.kv;
                Util.closeIfPossible(ldb);
                Util.closeIfPossible(ldb.getDB());
            }
            this.kv = null;
        }
        Util.closeIfPossible(this.logDirChannel);
        this.logDirChannel = null;
        this.raftLog.clear();
        this.random = null;
        this.network.stop();
        this.currentTerm = 0;
        this.commitIndex = 0;
        this.clusterId = 0;
        this.lastAppliedTerm = 0;
        this.lastAppliedIndex = 0;
        this.lastAppliedConfig = null;
        this.currentConfig = null;
        this.transmitting.clear();
        this.pendingService.clear();
        this.shuttingDown = false;
    }

    /**
     * Initialize our in-memory state from the persistent state reloaded from disk.
     * This is invoked on initial startup.
     */
    private void loadLog() throws IOException {

        // Sanity check
        assert Thread.holdsLock(this);

        // Scan for log entry files
        this.raftLog.clear();
        for (File file : this.logDir.listFiles()) {

            // Is this a log entry file?
            if (LogEntry.LOG_FILE_PATTERN.matcher(file.getName()).matches()) {
                this.info("recovering log file " + file.getName());
                this.raftLog.add(LogEntry.fromFile(file));
                continue;
            }

            // Is this a leftover temporary file?
            if (TEMP_FILE_PATTERN.matcher(file.getName()).matches()) {
                this.info("deleting leftover temporary file " + file.getName());
                if (!file.delete())
                    this.error("failed to delete leftover temporary file " + file);
                continue;
            }

            // Is this a KV store directory (expected)?
            if (KVSTORE_FILE_PATTERN.matcher(file.getName()).matches())
                continue;

            // Unknown
            this.warn("ignoring unrecognized file " + file.getName() + " in my log directory");
        }

        // Verify we have a contiguous range of log entries starting from the snapshot index; discard bogus log files
        Collections.sort(this.raftLog, LogEntry.SORT_BY_INDEX);
        long lastTermSeen = this.lastAppliedTerm;
        long expectedIndex = this.lastAppliedIndex + 1;
        for (Iterator<LogEntry> i = this.raftLog.iterator(); i.hasNext(); ) {
            final LogEntry logEntry = i.next();
            String error = null;
            if (logEntry.getTerm() < lastTermSeen)
                error = "term " + logEntry.getTerm() + " < last applied term " + lastTermSeen;
            else if (logEntry.getIndex() < this.lastAppliedIndex)
                error = "index " + logEntry.getIndex() + " < last applied index " + this.lastAppliedIndex;
            else if (logEntry.getIndex() != expectedIndex)
                error = "index " + logEntry.getIndex() + " != expected index " + expectedIndex;
            if (error != null) {
                this.warn("deleting bogus log file " + logEntry.getFile().getName() + ": " + error);
                if (!logEntry.getFile().delete())
                    this.error("failed to delete bogus log file " + logEntry.getFile());
                i.remove();
            } else {
                expectedIndex++;
                lastTermSeen = logEntry.getTerm();
            }
        }
        this.info("recovered " + this.raftLog.size() + " Raft log entries: " + this.raftLog);

        // Rebuild current configuration
        this.currentConfig = this.buildCurrentConfig();
    }

    private Map<String, String> buildCurrentConfig() {

        // Start with last applied config
        final HashMap<String, String> config = new HashMap<>(this.lastAppliedConfig);

        // Apply any changes found in uncommitted log entries
        for (LogEntry logEntry : this.raftLog)
            logEntry.applyConfigChange(config);

        // Done
        return config;
    }

// Configuration Stuff

    /**
     * Retrieve the unique 32-bit ID for this node's cluster.
     *
     * <p>
     * A value of zero indicates an unconfigured system. Usually the reverse true, though an unconfigured system
     * can have a non-zero cluster ID in the rare case where an error occurred persisting the initial log entry.
     *
     * @return unique cluster ID, or zero if this node is unconfigured
     */
    public synchronized int getClusterId() {
        return this.clusterId;
    }

    /**
     * Retrieve the current cluster configuration as understood by this node.
     *
     * <p>
     * Configuration changes are performed and committed in the context of a normal transaction; see
     * {@link RaftKVTransaction#configChange RaftKVTransaction.configChange()}.
     *
     * <p>
     * If this system is unconfigured, an empty map is returned (and vice-versa).
     *
     * @return current configuration mapping from node identity to network address, or empty if this node is unconfigured
     */
    public synchronized Map<String, String> getCurrentConfig() {
        return new HashMap<>(this.currentConfig);
    }

    /**
     * Determine whether this instance is configured.
     *
     * <p>
     * A node is configured iff it has added at least one Raft log entry (because the first log entry in any
     * new cluster always includes a configuration change that adds the first node in the cluster).
     *
     * @return true if this instance is configured, otherwise false
     */
    public boolean isConfigured() {
        return this.lastAppliedIndex > 0 || !this.raftLog.isEmpty();
    }

    /**
     * Determine whether this node thinks that it is part of the cluster, as currently configured on this node.
     *
     * @return true if this instance is part of the cluster, otherwise false
     */
    public boolean isClusterMember() {
        return this.isClusterMember(this.identity);
    }

    /**
     * Determine whether this node thinks that the specified node is part of the cluster, as currently configured on this node.
     *
     * @param node node identity
     * @return true if the specified node is part of the cluster, otherwise false
     */
    public boolean isClusterMember(String node) {
        return this.currentConfig.containsKey(node);
    }

// Transactions

    /**
     * Create a new transaction.
     *
     * @throws IllegalStateException if this instance is not {@linkplain #start started} or in the process of shutting down
     */
    @Override
    public synchronized RaftKVTransaction createTransaction() {

        // Sanity check
        assert this.checkState();
        Preconditions.checkState(this.role != null, "not started");
        Preconditions.checkState(!this.shuttingDown, "shutting down");

        // Base transaction on the most recent log entry. This is itself a form of optimistic locking: we assume that the
        // most recent log entry has a high probability of being committed (in the Raft sense), which is of course required
        // in order to commit any transaction based on it.
        final MostRecentView view = new MostRecentView();

        // Create transaction
        final RaftKVTransaction tx = new RaftKVTransaction(this,
          this.getLastLogTerm(), this.getLastLogIndex(), view.getSnapshot(), view.getView());
        if (this.log.isDebugEnabled())
            this.debug("created new transaction " + tx);
        this.openTransactions.put(tx.getTxId(), tx);

        // Set default commit timeout
        tx.setTimeout(this.commitTimeout);

        // Done
        return tx;
    }

    /**
     * Commit a transaction.
     */
    void commit(final RaftKVTransaction tx) {
        try {

            // Mark transaction as "commit ready" - service thread will do the rest
            synchronized (this) {

                // Sanity check
                assert this.checkState();
                assert this.role != null;

                // Check tx state
                switch (tx.getState()) {
                case EXECUTING:

                    // Transition to COMMIT_READY state
                    if (this.log.isDebugEnabled())
                        this.debug("committing transaction " + tx);
                    tx.closeSnapshot();
                    tx.setState(TxState.COMMIT_READY);
                    this.requestService(this.role.new CheckReadyTransactionService(tx));

                    // Setup commit timer
                    final int timeout = tx.getTimeout();
                    if (timeout != 0) {
                        final Timer commitTimer = new Timer("commit timer for " + tx,
                          new Service(null, "commit timeout for tx#" + tx.getTxId()) {
                            @Override
                            public void run() {
                                switch (tx.getState()) {
                                case COMMIT_READY:
                                case COMMIT_WAITING:
                                    RaftKVDatabase.this.fail(tx, new RetryTransactionException(tx,
                                      "transaction failed to complete within " + timeout + "ms (in state " + tx.getState() + ")"));
                                    break;
                                default:
                                    break;
                                }
                            }
                        });
                        commitTimer.timeoutAfter(tx.getTimeout());
                        tx.setCommitTimer(commitTimer);
                    }
                    break;
                case CLOSED:                                        // this transaction has already been committed or rolled back
                    throw new StaleTransactionException(tx);
                default:                                            // another thread is already doing the commit
                    this.warn("simultaneous commit()'s requested for " + tx + " by two different threads");
                    break;
                }
            }

            // Wait for completion
            try {
                tx.getCommitFuture().get();
            } catch (InterruptedException e) {
                throw new RetryTransactionException(tx, "thread interrupted while waiting for commit", e);
            } catch (ExecutionException e) {
                final Throwable cause = e.getCause();
                Util.prependCurrentStackTrace(cause, "Asynchronous Commit");
                if (cause instanceof Error)
                    throw (Error)cause;
                if (cause instanceof RuntimeException)
                    throw (RuntimeException)cause;
                throw new KVTransactionException(tx, "commit failed", cause);           // should never get here
            }
        } finally {
            this.cleanupTransaction(tx);
        }
    }

    /**
     * Rollback a transaction.
     */
    synchronized void rollback(RaftKVTransaction tx) {

        // Sanity check
        assert this.checkState();
        assert this.role != null;

        // Check tx state
        switch (tx.getState()) {
        case EXECUTING:
            if (this.log.isDebugEnabled())
                this.debug("rolling back transaction " + tx);
            this.cleanupTransaction(tx);
            break;
        case CLOSED:
            break;
        default:                                            // another thread is currently committing!
            this.warn("simultaneous commit() and rollback() requested for " + tx + " by two different threads");
            break;
        }
    }

    private synchronized void cleanupTransaction(RaftKVTransaction tx) {

        // Debug
        if (this.log.isTraceEnabled())
            this.trace("cleaning up transaction " + tx);

        // Do any per-role cleanups
        if (this.role != null)
            this.role.cleanupForTransaction(tx);

        // Close transaction's snapshot
        tx.closeSnapshot();

        // Cancel commit timer
        final Timer commitTimer = tx.getCommitTimer();
        if (commitTimer != null)
            commitTimer.cancel();

        // Remove from open transactions set
        this.openTransactions.remove(tx.getTxId());

        // Transition to CLOSED
        tx.setState(TxState.CLOSED);

        // Notify waiting thread if doing shutdown
        if (this.shuttingDown)
            this.notify();
    }

    // Mark a transaction as having succeeded
    void succeed(RaftKVTransaction tx) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert this.role != null;
        if (tx.getState().equals(TxState.COMPLETED) || tx.getState().equals(TxState.CLOSED))
            return;

        // Succeed transaction
        if (this.log.isDebugEnabled())
            this.debug("successfully committed " + tx);
        tx.getCommitFuture().succeed();
        tx.setState(TxState.COMPLETED);
        this.role.cleanupForTransaction(tx);
    }

    // Mark a transaction as having failed
    void fail(RaftKVTransaction tx, Exception e) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert this.role != null;
        if (tx.getState().equals(TxState.COMPLETED) || tx.getState().equals(TxState.CLOSED))
            return;

        // Fail transaction
        if (this.log.isDebugEnabled())
            this.debug("failed transaction " + tx + ": " + e);
        tx.getCommitFuture().fail(e);
        tx.setState(TxState.COMPLETED);
        this.role.cleanupForTransaction(tx);
    }

// DataView

    /**
     * A view of the database based on the most recent log entry, if any, otherwise directly on the committed key/value store.
     * Caller is responsible for eventually closing the snapshot.
     */
    private class MostRecentView {

        private final CloseableKVStore snapshot;
        private final MutableView view;

        public MostRecentView() {
            assert Thread.holdsLock(RaftKVDatabase.this);

            // Grab a snapshot of the key/value store
            this.snapshot = RaftKVDatabase.this.kv.snapshot();

            // Create a view of just the state machine keys and values and successively layer unapplied log entries
            KVStore kview = PrefixKVStore.create(snapshot, STATE_MACHINE_PREFIX);
            for (LogEntry logEntry : RaftKVDatabase.this.raftLog) {
                final Writes writes = logEntry.getWrites();
                if (!writes.isEmpty())
                    kview = new MutableView(kview, null, logEntry.getWrites());
            }

            // Finalize
            this.view = new MutableView(kview);
        }

        public CloseableKVStore getSnapshot() {
            return this.snapshot;
        }

        public MutableView getView() {
            return this.view;
        }
    }

// Service

    /**
     * Request service to be invoked after the current service (if any) completes.
     *
     * <p>
     * If {@code service} has an associated {@link Role}, and that {@link Role} is no longer active
     * when the service is handled, nothing will be done.
     *
     * @param service the service to perform
     */
    private void requestService(Service service) {
        assert Thread.holdsLock(this);
        if (!this.pendingService.add(service))
            return;
        if (this.performingService)
            return;
        try {
            this.executor.submit(new ErrorLoggingRunnable() {
                @Override
                protected void doRun() {
                    RaftKVDatabase.this.handlePendingService();
                }
            });
        } catch (RejectedExecutionException e) {
            if (!this.shuttingDown)
                this.warn("executor task rejected, skipping", e);
        }
    }

    // Performs pending service requests (do not invoke directly)
    private synchronized void handlePendingService() {

        // Sanity check
        assert this.checkState();
        assert Thread.currentThread() instanceof ServiceThread;
        if (this.role == null)
            return;

        // While there is work to do, do it
        this.performingService = true;
        try {
            while (!this.pendingService.isEmpty()) {
                final Iterator<Service> i = this.pendingService.iterator();
                final Service service = i.next();
                i.remove();
                assert service.getRole() == null || service.getRole() == this.role;
                if (this.log.isTraceEnabled())
                    this.trace("SERVICE [" + service + "] in " + this.role);
                service.run();
            }
        } finally {
            this.performingService = false;
        }
    }

    private class ServiceThread extends Thread {
        ServiceThread(Runnable action) {
            super(action);
            this.setName(RaftKVDatabase.this + " Service");
        }
    }

    private abstract static class Service implements Runnable {

        private final Role role;
        private final String desc;

        protected Service(Role role, String desc) {
            this.role = role;
            this.desc = desc;
        }

        public Role getRole() {
            return this.role;
        }

        @Override
        public String toString() {
            return this.desc;
        }
    }

// Timer

    /**
     * One shot timer that {@linkplain #requestService requests} a {@link Service} on expiration.
     *
     * <p>
     * This implementation avoids any race conditions between scheduling, firing, and cancelling.
     */
    class Timer {

        private final Logger log = RaftKVDatabase.this.log;
        private final String name;
        private final Service service;
        private ScheduledFuture<?> future;
        private PendingTimeout pendingTimeout;                  // non-null IFF timeout has not been handled yet
        private Timestamp timeoutDeadline;

        public Timer(String name, Service service) {
            this.name = name;
            this.service = service;
        }

        /**
         * Stop timer if running.
         *
         * @throws IllegalStateException if the lock object is not locked
         */
        public void cancel() {

            // Sanity check
            assert Thread.holdsLock(RaftKVDatabase.this);

            // Cancel existing timer, if any
            if (this.future != null) {
                this.future.cancel(false);
                this.future = null;
            }

            // Ensure the previously scheduled action does nothing if case we lose the cancel() race condition
            this.pendingTimeout = null;
            this.timeoutDeadline = null;
        }

        /**
         * (Re)schedule this timer. Discards any previously scheduled timeout.
         *
         * @param delay delay before expiration in milliseonds
         * @return true if restarted, false if executor rejected the task
         * @throws IllegalStateException if the lock object is not locked
         */
        public void timeoutAfter(int delay) {

            // Sanity check
            assert Thread.holdsLock(RaftKVDatabase.this);
            Preconditions.checkArgument(delay >= 0, "delay < 0");

            // Cancel existing timeout action, if any
            this.cancel();
            assert this.future == null;
            assert this.pendingTimeout == null;
            assert this.timeoutDeadline == null;

            // Reschedule new timeout action
            this.timeoutDeadline = new Timestamp().offset(delay);
            if (this.log.isTraceEnabled()) {
                RaftKVDatabase.this.trace("rescheduling " + this.name + " for " + this.timeoutDeadline
                  + " (" + delay + "ms from now)");
            }
            this.pendingTimeout = new PendingTimeout();
            try {
                this.future = RaftKVDatabase.this.executor.schedule(this.pendingTimeout, delay, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                if (!RaftKVDatabase.this.shuttingDown)
                    RaftKVDatabase.this.warn("can't restart timer", e);
            }
        }

        /**
         * Force timer to expire immediately.
         */
        public void timeoutNow() {
            this.timeoutAfter(0);
        }

        /**
         * Determine if this timer has expired and requires service handling, and reset it if so.
         *
         * <p>
         * If this timer is not running, has not yet expired, or has previously expired and this method was already
         * thereafter invoked, false is returned. Otherwise, true is returned, this timer is {@link #cancel}ed (if necessary),
         * and the caller is expected to handle the implied service need.
         *
         * @return true if timer needs handling, false otherwise
         */
        public boolean pollForTimeout() {

            // Sanity check
            assert Thread.holdsLock(RaftKVDatabase.this);

            // Has timer expired?
            if (this.pendingTimeout == null || !this.timeoutDeadline.hasOccurred())
                return false;

            // Yes, timer requires service
            if (Timer.this.log.isTraceEnabled())
                RaftKVDatabase.this.trace(Timer.this.name + " expired " + -this.timeoutDeadline.offsetFromNow() + "ms ago");
            this.cancel();
            return true;
        }

        /**
         * Determine if this timer is running, i.e., will expire or has expired but
         * {@link #pollForTimeout} has not been invoked yet.
         */
        public boolean isRunning() {
            return this.pendingTimeout != null;
        }

        private class PendingTimeout extends ErrorLoggingRunnable {

            @Override
            protected void doRun() {
                synchronized (RaftKVDatabase.this) {

                    // Avoid cancel() race condition
                    if (Timer.this.pendingTimeout != this)
                        return;

                    // Trigger service
                    RaftKVDatabase.this.requestService(Timer.this.service);
                }
            }
        }
    }

// Raft stuff

    /**
     * Reset the persisted state machine to its initial state.
     */
    private boolean resetStateMachine(boolean initialize) {

        // Sanity check
        assert Thread.holdsLock(this);
        if (this.log.isDebugEnabled())
            this.debug("resetting state machine");

        // Set invalid values while we make non-atomic changes, in case we crash in the middle
        if (!this.recordLastApplied(-1, -1, null))
            return false;

        // Delete all key/value pairs
        this.kv.removeRange(STATE_MACHINE_PREFIX, ByteUtil.getKeyAfterPrefix(STATE_MACHINE_PREFIX));
        assert !this.kv.getRange(STATE_MACHINE_PREFIX, ByteUtil.getKeyAfterPrefix(STATE_MACHINE_PREFIX), false).hasNext();

        // Delete all log files
        this.raftLog.clear();
        for (File file : this.logDir.listFiles()) {
            if (LogEntry.LOG_FILE_PATTERN.matcher(file.getName()).matches()) {
                if (!file.delete())
                    this.error("failed to delete log file " + file);
            }
        }

        // Optionally finish intialization
        if (initialize && !this.recordLastApplied(0, 0, null))
            return false;

        // Done
        if (this.log.isDebugEnabled())
            this.debug("done resetting state machine");
        return true;
    }

    /**
     * Record the last applied term, index, and configuration in the persistent store.
     */
    private boolean recordLastApplied(long term, long index, Map<String, String> config) {

        // Sanity check
        assert Thread.holdsLock(this);
        if (this.log.isTraceEnabled())
            this.trace("updating state machine last applied to " + index + "t" + term + " with config " + config);
        if (config == null)
            config = new HashMap<String, String>(0);

        // Prepare updates
        final Writes writes = new Writes();
        writes.getPuts().put(LAST_APPLIED_TERM_KEY, LongEncoder.encode(term));
        writes.getPuts().put(LAST_APPLIED_INDEX_KEY, LongEncoder.encode(index));
        writes.getPuts().put(LAST_APPLIED_CONFIG_KEY, this.encodeConfig(config));

        // Update persistent store
        try {
            this.kv.mutate(writes, true);
        } catch (Exception e) {
            this.error("error updating key/value store term/index to " + index + "t" + term, e);
            return false;
        }

        // Update in-memory copy
        this.lastAppliedTerm = Math.max(term, 0);
        this.lastAppliedIndex = Math.max(index, 0);
        this.lastAppliedConfig = config;
        this.commitIndex = this.lastAppliedIndex;
        this.currentConfig = this.buildCurrentConfig();
        if (term >= 0 && index >= 0)
            this.info("new current configuration: " + this.currentConfig);
        return true;
    }

    /**
     * Update and persist a new current term.
     */
    private boolean advanceTerm(long newTerm) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert newTerm > this.currentTerm;
        this.info("advancing current term from " + this.currentTerm + " -> " + newTerm);

        // Update persistent store
        final Writes writes = new Writes();
        writes.getPuts().put(CURRENT_TERM_KEY, LongEncoder.encode(newTerm));
        writes.setRemoves(new KeyRanges(VOTED_FOR_KEY));
        try {
            this.kv.mutate(writes, true);
        } catch (Exception e) {
            this.error("error persisting new term " + newTerm, e);
            return false;
        }

        // Update in-memory copy
        this.currentTerm = newTerm;
        return true;
    }

    /**
     * Join the specified cluster.
     *
     * @param newClusterId cluster ID, or zero to have one randomly assigned
     */
    private boolean joinCluster(int newClusterId) {

        // Sanity check
        assert Thread.holdsLock(this);
        Preconditions.checkState(this.clusterId == 0);

        // Pick a new, random cluster ID if needed
        while (newClusterId == 0)
            newClusterId = this.random.nextInt();

        // Persist it
        this.info("joining cluster with ID " + String.format("0x%08x", newClusterId));
        final Writes writes = new Writes();
        writes.getPuts().put(CLUSTER_ID_KEY, LongEncoder.encode(newClusterId));
        try {
            this.kv.mutate(writes, true);
        } catch (Exception e) {
            this.error("error updating key/value store with new cluster ID", e);
            return false;
        }

        // Done
        this.clusterId = newClusterId;
        return true;
    }

    /**
     * Set the Raft role.
     *
     * @param role new role
     */
    private void changeRole(Role role) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert role != null;

        // Shutdown previous role (if any)
        if (this.role != null) {
            this.role.shutdown();
            for (Iterator<Service> i = this.pendingService.iterator(); i.hasNext(); ) {
                final Service service = i.next();
                if (service.getRole() != null)
                    i.remove();
            }
        }

        // Setup new role
        this.role = role;
        this.role.setup();
        this.info("changing role to " + role);
    }

    /**
     * Append a log entry to the Raft log.
     *
     * @param term new log entry term
     * @param entry entry to add; the {@linkplain NewLogEntry#getTempFile temporary file} must be already durably persisted,
     *  and will be renamed
     * @return new {@link LogEntry}
     * @throws Exception if an error occurs
     */
    private LogEntry appendLogEntry(long term, NewLogEntry newLogEntry) throws Exception {

        // Sanity check
        assert Thread.holdsLock(this);
        assert this.role != null;
        assert newLogEntry != null;

        // Get file length
        final LogEntry.Data data = newLogEntry.getData();
        final File tempFile = newLogEntry.getTempFile();
        final long fileLength = Util.getLength(tempFile);

        // Create new log entry
        final LogEntry logEntry = new LogEntry(term, this.getLastLogIndex() + 1, this.logDir, data, fileLength);
        if (this.log.isDebugEnabled())
            this.debug("adding new log entry " + logEntry + " using " + tempFile.getName());

        // Atomically rename file and fsync() directory to durably persist
        Files.move(tempFile.toPath(), logEntry.getFile().toPath(), StandardCopyOption.ATOMIC_MOVE);
        this.logDirChannel.force(true);

        // Add new log entry to in-memory log
        this.raftLog.add(logEntry);

        // Update current config
        if (logEntry.applyConfigChange(this.currentConfig))
            this.info("new current configuration: " + this.currentConfig);

        // Done
        return logEntry;
    }

    private long getLastLogIndex() {
        assert Thread.holdsLock(this);
        return this.lastAppliedIndex + this.raftLog.size();
    }

    private long getLastLogTerm() {
        assert Thread.holdsLock(this);
        return this.getLogTermAtIndex(this.getLastLogIndex());
    }

    private long getLogTermAtIndex(long index) {
        assert Thread.holdsLock(this);
        assert index >= this.lastAppliedIndex;
        assert index <= this.getLastLogIndex();
        return index == this.lastAppliedIndex ? this.lastAppliedTerm : this.getLogEntryAtIndex(index).getTerm();
    }

    private LogEntry getLogEntryAtIndex(long index) {
        assert Thread.holdsLock(this);
        assert index > this.lastAppliedIndex;
        assert index <= this.getLastLogIndex();
        return this.raftLog.get((int)(index - this.lastAppliedIndex - 1));
    }

    /**
     * Contains the information required to commit a new entry to the log.
     */
    private class NewLogEntry {

        private final LogEntry.Data data;
        private final File tempFile;

        /**
         * Create an instance from a transaction and a temporary file
         *
         * @param data log entry mutations
         * @throws Exception if an error occurs
         */
        public NewLogEntry(RaftKVTransaction tx, File tempFile) throws IOException {
            this.data = new LogEntry.Data(tx.getMutableView().getWrites(), tx.getConfigChange());
            this.tempFile = tempFile;
        }

        /**
         * Create an instance from a transaction.
         *
         * @param data log entry mutations
         * @throws Exception if an error occurs
         */
        public NewLogEntry(RaftKVTransaction tx) throws IOException {
            this.data = new LogEntry.Data(tx.getMutableView().getWrites(), tx.getConfigChange());
            this.tempFile = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX, RaftKVDatabase.this.logDir);
            try (FileWriter output = new FileWriter(this.tempFile)) {
                LogEntry.writeData(output, data);
            }
        }

        /**
         * Create an instance from a {@link LogEntry.Data} object.
         *
         * @param data mutation data
         * @throws Exception if an error occurs
         */
        public NewLogEntry(LogEntry.Data data) throws IOException {
            this.data = data;
            this.tempFile = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX, RaftKVDatabase.this.logDir);
            try (FileWriter output = new FileWriter(this.tempFile)) {
                LogEntry.writeData(output, data);
            }
        }

        /**
         * Create an instance from a serialized data in a {@link ByteBuffer}.
         *
         * @param buf buffer containing serialized mutations
         * @throws Exception if an error occurs
         */
        public NewLogEntry(ByteBuffer dataBuf) throws IOException {

            // Copy data to temporary file
            this.tempFile = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX, RaftKVDatabase.this.logDir);
            try (FileWriter output = new FileWriter(this.tempFile)) {
                while (dataBuf.hasRemaining())
                    output.getFileOutputStream().getChannel().write(dataBuf);
            }

            // Avoid having two copies of the data in memory at once
            dataBuf = null;

            // Deserialize data from file back into memory
            try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(tempFile), 4096)) {
                this.data = LogEntry.readData(input);
            }
        }

        public LogEntry.Data getData() {
            return this.data;
        }

        public File getTempFile() {
            return this.tempFile;
        }

        public void cancel() {
            this.tempFile.delete();
        }
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[identity=" + (this.identity != null ? "\"" + this.identity + "\"" : null)
          + ",logDir=" + this.logDir
          + ",term=" + this.currentTerm
          + ",commitIndex=" + this.commitIndex
          + ",lastApplied=" + this.lastAppliedIndex + "t" + this.lastAppliedTerm
          + ",raftLog=" + this.raftLog
          + ",role=" + this.role
          + (this.shuttingDown ? ",shuttingDown" : "")
          + "]";
    }

// Network.Handler and Messaging

    private void handle(String sender, ByteBuffer buf) {

        // Decode message
        final Message msg;
        try {
            msg = Message.decode(buf);
        } catch (IllegalArgumentException e) {
            this.error("rec'd bogus message from " + sender + ", ignoring", e);
            return;
        }

        // Receive message
        this.receiveMessage(sender, msg);
    }

    private synchronized void outputQueueEmpty(String address) {

        // Sanity check
        assert this.checkState();

        // Update transmitting status
        if (!this.transmitting.remove(address))
            return;
        if (this.log.isTraceEnabled())
            this.trace("QUEUE_EMPTY address " + address + " in " + this.role);

        // Notify role
        if (this.role == null)
            return;
        this.role.outputQueueEmpty(address);
    }

    private boolean isTransmitting(String address) {
        return this.transmitting.contains(address);
    }

// Messages

    private boolean sendMessage(Message msg) {

        // Sanity check
        assert Thread.holdsLock(this);

        // Get peer's address; if unknown, use the return address of the message being processed (if any)
        final String peer = msg.getRecipientId();
        String address = this.currentConfig.get(peer);
        if (address == null)
            address = this.returnAddress;
        if (address == null) {
            this.warn("can't send " + msg + " to unknown peer \"" + peer + "\"");
            return false;
        }

        // Send message
        if (this.log.isTraceEnabled())
            this.trace("XMIT " + msg + " to " + address);
        if (this.network.send(address, msg.encode())) {
            this.transmitting.add(address);
            return true;
        }

        // Transmit failed
        this.warn("transmit of " + msg + " to \"" + peer + "\" failed locally");
        return false;
    }

    private synchronized void receiveMessage(String address, Message msg) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert this.checkState();
        if (this.role == null) {
            if (this.log.isDebugEnabled())
                this.debug("rec'd " + msg + " rec'd in shutdown state; ignoring");
            return;
        }

        // Sanity check cluster ID
        if (msg.getClusterId() == 0) {
            this.warn("rec'd " + msg + " with zero cluster ID from " + address + "; ignoring");
            return;
        }
        if (this.clusterId != 0 && msg.getClusterId() != this.clusterId) {
            this.warn("rec'd " + msg + " with foreign cluster ID "
              + String.format("0x%08x", msg.getClusterId()) + " != " + String.format("0x%08x", this.clusterId) + "; ignoring");
            return;
        }

        // Sanity check sender
        final String peer = msg.getSenderId();
        if (peer.equals(this.identity)) {
            this.warn("rec'd " + msg + " from myself (\"" + peer + "\", address " + address + "); ignoring");
            return;
        }

        // Sanity check recipient
        final String dest = msg.getRecipientId();
        if (!dest.equals(this.identity)) {
            this.warn("rec'd misdirected " + msg + " intended for \"" + dest + "\" from " + address + "; ignoring");
            return;
        }

        // Is sender's term too low? Ignore it
        if (msg.getTerm() < this.currentTerm) {
            this.info("rec'd " + msg + " with term " + msg.getTerm() + " < " + this.currentTerm + " from \""
              + peer + "\" at " + address + ", ignoring");
            return;
        }

        // Is my term too low? If so update and revert to follower
        if (msg.getTerm() > this.currentTerm) {

            // First check with current role; in some special cases we ignore this
            if (!this.role.mayAdvanceCurrentTerm(msg)) {
                if (this.log.isTraceEnabled()) {
                    this.trace("rec'd " + msg + " with term " + msg.getTerm() + " > " + this.currentTerm + " from \""
                      + peer + "\" but current role says to ignore it");
                }
                return;
            }

            // Revert to follower
            this.info("rec'd " + msg.getClass().getSimpleName() + " with term " + msg.getTerm() + " > " + this.currentTerm
              + " from \"" + peer + "\", updating term and "
              + (this.role instanceof FollowerRole ? "remaining a" : "reverting to") + " follower");
            if (!this.advanceTerm(msg.getTerm()))
                return;
            this.changeRole(msg.isLeaderMessage() ? new FollowerRole(this, peer, address) : new FollowerRole(this));
        }

        // Debug
        if (this.log.isTraceEnabled())
            this.trace("RECV " + msg + " in " + this.role + " from " + address);

        // Handle message
        this.returnAddress = address;
        try {
            msg.visit(this.role);
        } finally {
            this.returnAddress = null;
        }
    }

// Utility methods

    private long decodeLong(byte[] key, long defaultValue) throws IOException {
        final byte[] value = this.kv.get(key);
        if (value == null)
            return defaultValue;
        try {
            return LongEncoder.decode(value);
        } catch (IllegalArgumentException e) {
            throw new IOException("can't interpret encoded long value "
              + ByteUtil.toString(value) + " under key " + ByteUtil.toString(key), e);
        }
    }

    private String decodeString(byte[] key, String defaultValue) throws IOException {
        final byte[] value = this.kv.get(key);
        if (value == null)
            return defaultValue;
        final DataInputStream input = new DataInputStream(new ByteArrayInputStream(value));
        try {
            return input.readUTF();
        } catch (IOException e) {
            throw new IOException("can't interpret encoded string value "
              + ByteUtil.toString(value) + " under key " + ByteUtil.toString(key), e);
        }
    }

    private byte[] encodeString(String value) {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        final DataOutputStream output = new DataOutputStream(buf);
        try {
            output.writeUTF(value);
            output.flush();
        } catch (IOException e) {
            throw new RuntimeException("unexpected error", e);
        }
        return buf.toByteArray();
    }

    private Map<String, String> decodeConfig(byte[] key) throws IOException {
        final Map<String, String> config = new HashMap<>();
        final byte[] value = this.kv.get(key);
        if (value == null)
            return config;
        try {
            final DataInputStream data = new DataInputStream(new ByteArrayInputStream(value));
            while (true) {
                data.mark(1);
                if (data.read() == -1)
                    break;
                data.reset();
                config.put(data.readUTF(), data.readUTF());
            }
        } catch (IOException e) {
            throw new IOException("can't interpret encoded config "
              + ByteUtil.toString(value) + " under key " + ByteUtil.toString(key), e);
        }
        return config;
    }

    private byte[] encodeConfig(Map<String, String> config) {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        final DataOutputStream data = new DataOutputStream(buf);
        try {
            for (Map.Entry<String, String> entry : config.entrySet()) {
                data.writeUTF(entry.getKey());
                data.writeUTF(entry.getValue());
            }
            data.flush();
        } catch (IOException e) {
            throw new RuntimeException("unexpected error", e);
        }
        return buf.toByteArray();
    }

// Logging

    private void trace(String msg, Throwable t) {
        this.log.trace(String.format("%s %s: %s", new Timestamp(), this.identity, msg), t);
    }

    private void trace(String msg) {
        this.log.trace(String.format("%s %s: %s", new Timestamp(), this.identity, msg));
    }

    private void debug(String msg, Throwable t) {
        this.log.debug(String.format("%s %s: %s", new Timestamp(), this.identity, msg), t);
    }

    private void debug(String msg) {
        this.log.debug(String.format("%s %s: %s", new Timestamp(), this.identity, msg));
    }

    private void info(String msg, Throwable t) {
        this.log.info(String.format("%s %s: %s", new Timestamp(), this.identity, msg), t);
    }

    private void info(String msg) {
        this.log.info(String.format("%s %s: %s", new Timestamp(), this.identity, msg));
    }

    private void warn(String msg, Throwable t) {
        this.log.warn(String.format("%s %s: %s", new Timestamp(), this.identity, msg), t);
    }

    private void warn(String msg) {
        this.log.warn(String.format("%s %s: %s", new Timestamp(), this.identity, msg));
    }

    private void error(String msg, Throwable t) {
        this.log.error(String.format("%s %s: %s", new Timestamp(), this.identity, msg), t);
    }

    private void error(String msg) {
        this.log.error(String.format("%s %s: %s", new Timestamp(), this.identity, msg));
    }

// Raft Roles

    private abstract static class Role implements MessageSwitch {

        protected final Logger log = LoggerFactory.getLogger(this.getClass());
        protected final RaftKVDatabase raft;
        protected final Service checkReadyTransactionsService = new Service(this, "check ready transactions") {
            @Override
            public void run() {
                Role.this.checkReadyTransactions();
            }
        };
        protected final Service checkWaitingTransactionsService = new Service(this, "check waiting transactions") {
            @Override
            public void run() {
                Role.this.checkWaitingTransactions();
            }
        };
        protected final Service applyCommittedLogEntriesService = new Service(this, "apply committed logs") {
            @Override
            public void run() {
                Role.this.applyCommittedLogEntries();
            }
        };

    // Constructors

        protected Role(RaftKVDatabase raft) {
            this.raft = raft;
            assert Thread.holdsLock(this.raft);
        }

    // Lifecycle

        public void setup() {
            assert Thread.holdsLock(this.raft);
            this.raft.requestService(this.checkReadyTransactionsService);
            this.raft.requestService(this.checkWaitingTransactionsService);
            this.raft.requestService(this.applyCommittedLogEntriesService);
        }

        public void shutdown() {
            assert Thread.holdsLock(this.raft);
            for (RaftKVTransaction tx : this.raft.openTransactions.values())
                this.cleanupForTransaction(tx);
        }

    // Service

        public abstract void outputQueueEmpty(String address);

        /**
         * Check transactions in the {@link TxState#COMMIT_READY} state to see if we can advance them.
         */
        protected void checkReadyTransactions() {
            for (RaftKVTransaction tx : new ArrayList<RaftKVTransaction>(this.raft.openTransactions.values()))
                new CheckReadyTransactionService(tx).run();
        }

        /**
         * Check transactions in the {@link TxState#COMMIT_WAITING} state to see if they are committed yet.
         * We invoke this service method whenever our {@code commitIndex} advances.
         */
        protected void checkWaitingTransactions() {
            for (RaftKVTransaction tx : new ArrayList<RaftKVTransaction>(this.raft.openTransactions.values()))
                new CheckWaitingTransactionService(tx).run();
        }

        /**
         * Apply committed but unapplied log entries to the state machine.
         * We invoke this service method whenever log entries are added or our {@code commitIndex} advances.
         */
        protected void applyCommittedLogEntries() {

            // Apply committed log entries to the state machine
            while (this.raft.lastAppliedIndex < this.raft.commitIndex) {

                // Grab the first unwritten log entry
                final LogEntry logEntry = this.raft.raftLog.get(0);
                assert logEntry.getIndex() == this.raft.lastAppliedIndex + 1;

                // Check with subclass
                if (!this.mayApplyLogEntry(logEntry))
                    break;

                // Get the current config as of the log entry we're about to apply
                final HashMap<String, String> logEntryConfig = new HashMap<>(this.raft.lastAppliedConfig);
                logEntry.applyConfigChange(logEntryConfig);

                // Prepare combined Mutations containing prefixed log entry changes plus my own
                final Writes logWrites = logEntry.getWrites();
                final Writes myWrites = new Writes();
                myWrites.getPuts().put(LAST_APPLIED_TERM_KEY, LongEncoder.encode(logEntry.getTerm()));
                myWrites.getPuts().put(LAST_APPLIED_INDEX_KEY, LongEncoder.encode(logEntry.getIndex()));
                myWrites.getPuts().put(LAST_APPLIED_CONFIG_KEY, this.raft.encodeConfig(logEntryConfig));
                final Mutations mutations = new Mutations() {

                    @Override
                    public Iterable<KeyRange> getRemoveRanges() {
                        return Iterables.transform(logWrites.getRemoveRanges(), new PrefixKeyRangeFunction(STATE_MACHINE_PREFIX));
                    }

                    @Override
                    public Iterable<Map.Entry<byte[], byte[]>> getPutPairs() {
                        return Iterables.concat(
                          Iterables.transform(logWrites.getPutPairs(), new PrefixPutFunction(STATE_MACHINE_PREFIX)),
                          myWrites.getPutPairs());
                    }

                    @Override
                    public Iterable<Map.Entry<byte[], Long>> getAdjustPairs() {
                        return Iterables.transform(logWrites.getAdjustPairs(), new PrefixAdjustFunction(STATE_MACHINE_PREFIX));
                    }
                };

                // Apply updates to the key/value store (durably); prefix all transaction keys with STATE_MACHINE_PREFIX
                if (this.log.isDebugEnabled())
                    this.debug("applying committed log entry " + logEntry + " to key/value store");
                try {
                    this.raft.kv.mutate(mutations, true);
                } catch (Exception e) {
                    if (e instanceof RuntimeException && e.getCause() instanceof IOException)
                        e = (IOException)e.getCause();
                    this.error("error applying log entry " + logEntry + " to key/value store", e);
                    break;
                }

                // Update in-memory state
                this.raft.lastAppliedTerm = logEntry.getTerm();
                assert logEntry.getIndex() == this.raft.lastAppliedIndex + 1;
                this.raft.lastAppliedIndex = logEntry.getIndex();
                logEntry.applyConfigChange(this.raft.lastAppliedConfig);
                assert this.raft.currentConfig.equals(this.raft.buildCurrentConfig());

                // Delete the log entry
                this.raft.raftLog.remove(0);
                if (!logEntry.getFile().delete())
                    this.error("failed to delete log file " + logEntry.getFile());

                // Subclass hook
                this.logEntryApplied(logEntry);
            }
        }

        /**
         * Determine whether the given log entry may be applied to the state machine.
         */
        protected boolean mayApplyLogEntry(LogEntry logEntry) {
            return true;
        }

        /**
         * Subclass hook invoked after a log entry has been applied to the state machine.
         *
         * @param logEntry the log entry just applied
         */
        protected void logEntryApplied(LogEntry logEntry) {
        }

    // Transaction service classes

        protected abstract class AbstractTransactionService extends Service {

            protected final RaftKVTransaction tx;

            AbstractTransactionService(RaftKVTransaction tx, String desc) {
                super(Role.this, desc);
                assert tx != null;
                this.tx = tx;
            }

            @Override
            public final void run() {
                try {
                    this.doRun();
                } catch (KVTransactionException e) {
                    Role.this.raft.fail(tx, e);
                } catch (Exception e) {
                    Role.this.raft.fail(tx, new KVTransactionException(tx, e));
                }
            }

            protected abstract void doRun();

            @Override
            public boolean equals(Object obj) {
                if (obj == null || obj.getClass() != this.getClass())
                    return false;
                final AbstractTransactionService that = (AbstractTransactionService)obj;
                return this.tx.equals(that.tx);
            }

            @Override
            public int hashCode() {
                return this.tx.hashCode();
            }
        }

        protected class CheckReadyTransactionService extends AbstractTransactionService {

            CheckReadyTransactionService(RaftKVTransaction tx) {
                super(tx, "check ready tx#" + tx.getTxId());
            }

            @Override
            protected void doRun() {
                if (this.tx.getState().equals(TxState.COMMIT_READY))
                    Role.this.checkReadyTransaction(this.tx);
            }
        }

        protected class CheckWaitingTransactionService extends AbstractTransactionService {

            CheckWaitingTransactionService(RaftKVTransaction tx) {
                super(tx, "check waiting tx#" + tx.getTxId());
            }

            @Override
            protected void doRun() {
                if (this.tx.getState().equals(TxState.COMMIT_WAITING))
                    Role.this.checkWaitingTransaction(this.tx);
            }
        }

    // Transactions

        /**
         * Check a transaction that is ready to be committed (in the {@link TxState#COMMIT_READY} state).
         *
         * <p>
         * This should be invoked:
         * <ul>
         *  <li>After changing roles</li>
         *  <li>After a transaction has entered the {@link TxState#COMMIT_READY} state</li>
         *  <li>After the leader is newly known (in {@link FollowerRole})</li>
         *  <li>After the leader's output queue goes from non-empty to empty (in {@link FollowerRole})</li>
         *  <li>After the leader's {@code commitIndex} has advanced, in case a config change transaction
         *      is waiting on a previous config change transaction (in {@link LeaderRole})</li>
         * </ul>
         *
         * @param tx the transaction
         * @throws KVTransactionException if an error occurs
         */
        public abstract void checkReadyTransaction(RaftKVTransaction tx);

        /**
         * Check a transaction waiting for its log entry to be committed (in the {@link TxState#COMMIT_WAITING} state).
         *
         * <p>
         * This should be invoked:
         * <ul>
         *  <li>After changing roles</li>
         *  <li>After a transaction has entered the {@link TxState#COMMIT_WAITING} state</li>
         *  <li>After advancing my {@code commitIndex} (as leader or follower)</li>
         *  <li>After receiving an updated {@linkplain AppendResponse#getLeaderLeaseTimeout leader lease timeout}
         *      (in {@link FollowerRole})</li>
         * </ul>
         *
         * @param tx the transaction
         * @throws KVTransactionException if an error occurs
         */
        private void checkWaitingTransaction(RaftKVTransaction tx) {

            // Handle the case the transaction's committed log index has already been applied to the state machine
            final long commitIndex = tx.getCommitIndex();
            if (commitIndex < this.raft.lastAppliedIndex) {

                // This can happen if we lose contact and by the time we're back the log entry has
                // already been applied to the state machine on some leader and that leader sent
                // use an InstallSnapshot message. We don't know whether it actually got committed
                // or not, so the transaction must be retried.
                throw new RetryTransactionException(tx, "committed log entry was missed");
            }

            // Has the transaction's log entry been received and committed yet?
            if (commitIndex > this.raft.commitIndex)
                return;

            // Verify the term of the committed log entry; if not what we expect, the log entry was overwritten by a new leader
            final long commitTerm = tx.getCommitTerm();
            if (this.raft.getLogTermAtIndex(commitIndex) != commitTerm)
                throw new RetryTransactionException(tx, "leader was deposed during commit and transaction's log entry overwritten");

            // Check with subclass
            if (!this.mayCommit(tx))
                return;

            // Transaction is officially committed now
            if (this.log.isTraceEnabled())
                this.trace("commit successful for " + tx + " (commit index " + this.raft.commitIndex + " >= " + commitIndex + ")");
            this.raft.succeed(tx);
        }

        protected boolean mayCommit(RaftKVTransaction tx) {
            return true;
        }

        /**
         * Perform any role-specific transaction cleanups.
         *
         * <p>
         * Invoked either when transaction is closed or this role is being shutdown.
         *
         * @param tx the transaction
         */
        public abstract void cleanupForTransaction(RaftKVTransaction tx);

        /**
         * Check a transaction that is ready to be committed (in the {@link TxState#COMMIT_READY} state) for snapshot isolation.
         *
         * @param tx the transaction
         * @return true if snapshot isolation and moved to {@link TxState@COMMIT_WAITING}
         */
        protected boolean checkReadyTransactionReadOnlySnapshot(RaftKVTransaction tx) {

            // Sanity check
            assert Thread.holdsLock(this.raft);
            assert tx.getState().equals(TxState.COMMIT_READY);

            // Check isolation
            if (!tx.isReadOnlySnapshot())
                return false;

            // For snapshot isolation, we only need to wait for the commit of the transaction's base log entry
            tx.setCommitTerm(tx.getBaseTerm());
            tx.setCommitIndex(tx.getBaseIndex());
            tx.setState(TxState.COMMIT_WAITING);
            this.raft.requestService(new CheckWaitingTransactionService(tx));

            // Done
            return true;
        }

    // Messages

        public boolean mayAdvanceCurrentTerm(Message msg) {
            return true;
        }

        protected void failUnexpectedMessage(Message msg) {
            this.warn("rec'd unexpected message " + msg + " while in role " + this + "; ignoring");
        }

    // Debug

        boolean checkState() {
            return true;
        }

    // Logging

        protected void trace(String msg, Throwable t) {
            this.raft.trace(msg, t);
        }

        protected void trace(String msg) {
            this.raft.trace(msg);
        }

        protected void debug(String msg, Throwable t) {
            this.raft.debug(msg, t);
        }

        protected void debug(String msg) {
            this.raft.debug(msg);
        }

        protected void info(String msg, Throwable t) {
            this.raft.info(msg, t);
        }

        protected void info(String msg) {
            this.raft.info(msg);
        }

        protected void warn(String msg, Throwable t) {
            this.raft.warn(msg, t);
        }

        protected void warn(String msg) {
            this.raft.warn(msg);
        }

        protected void error(String msg, Throwable t) {
            this.raft.error(msg, t);
        }

        protected void error(String msg) {
            this.raft.error(msg);
        }

    // Object

        @Override
        public abstract String toString();

        protected String toStringPrefix() {
            return this.getClass().getSimpleName()
              + "[term=" + this.raft.currentTerm
              + ",applied=" + this.raft.lastAppliedIndex + "t" + this.raft.lastAppliedTerm
              + ",commit=" + this.raft.commitIndex
              + ",log=" + this.raft.raftLog
              + "]";
        }
    }

// LEADER role

    private static class LeaderRole extends Role {

        // Our followers
        private final HashMap<String, Follower> followerMap = new HashMap<>();

        // Our leadership "lease" timeout - i.e., the earliest time another leader could possibly be elected
        private Timestamp leaseTimeout = new Timestamp();

        // Unapplied log entry memory usage
        private long totalLogEntryMemoryUsed;

        // Service tasks
        private final Service updateLeaderCommitIndexService = new Service(this, "update leader commitIndex") {
            @Override
            public void run() {
                LeaderRole.this.updateLeaderCommitIndex();
            }
        };
        private final Service updateLeaseTimeoutService = new Service(this, "update lease timeout") {
            @Override
            public void run() {
                LeaderRole.this.updateLeaseTimeout();
            }
        };
        private final Service updateKnownFollowersService = new Service(this, "update known followers") {
            @Override
            public void run() {
                LeaderRole.this.updateKnownFollowers();
            }
        };

    // Constructors

        public LeaderRole(RaftKVDatabase raft) {
            super(raft);
        }

    // Lifecycle

        @Override
        public void setup() {
            super.setup();
            this.info("entering leader role in term " + this.raft.currentTerm);

            // Generate follower list
            this.updateKnownFollowers();

            // Initialize log memory usage
            for (LogEntry logEntry : this.raft.raftLog)
                this.totalLogEntryMemoryUsed += logEntry.getFileSize();

            // Append a "dummy" log entry with my current term. This allows us to advance the commit index when the last
            // entry in our log is from a prior term. This is needed to avoid the problem where a transaction could end up
            // waiting indefinitely for its log entry with a prior term number to be committed.
            final LogEntry logEntry;
            try {
                logEntry = this.applyNewLogEntry(this.raft.new NewLogEntry(new LogEntry.Data(new Writes(), null)));
            } catch (Exception e) {
                this.error("error attempting to apply initial log entry", e);
                return;
            }
            if (this.log.isDebugEnabled())
                this.debug("added log entry " + logEntry + " to commit at the beginning of my new term");
        }

        @Override
        public void shutdown() {
            super.shutdown();
            for (Follower follower : this.followerMap.values())
                follower.cleanup();
        }

    // Service

        @Override
        public void outputQueueEmpty(String address) {

            // Find matching follower(s) and update them if needed
            for (Follower follower : this.followerMap.values()) {
                if (follower.getAddress().equals(address)) {
                    if (this.log.isTraceEnabled())
                        this.trace("updating peer \"" + follower.getIdentity() + "\" after queue empty notification");
                    this.raft.requestService(new UpdateFollowerService(follower));
                }
            }
        }

        @Override
        protected void logEntryApplied(LogEntry logEntry) {
            this.totalLogEntryMemoryUsed -= logEntry.getFileSize();
        }

        @Override
        protected boolean mayApplyLogEntry(LogEntry logEntry) {

            // Are we running out of memory, or keeping around too many log entries? If so, go ahead.
            if (this.totalLogEntryMemoryUsed > this.raft.maxAppliedLogMemory
              || this.raft.raftLog.size() > MAX_UNAPPLIED_LOG_ENTRIES) {
                if (this.log.isTraceEnabled()) {
                    this.trace("allowing log entry " + logEntry + " to be applied because memory usage "
                      + this.totalLogEntryMemoryUsed + " > " + this.raft.maxAppliedLogMemory + " and/or log length "
                      + this.raft.raftLog.size() + " > " + MAX_UNAPPLIED_LOG_ENTRIES);
                }
                return true;
            }

            // Try to keep log entries around for a minimum amount of time to facilitate long-running transactions
            if (logEntry.getAge() < this.raft.maxTransactionDuration) {
                if (this.log.isTraceEnabled()) {
                    this.trace("delaying application of " + logEntry + " because it has age "
                      + logEntry.getAge() + "ms < " + this.raft.maxTransactionDuration + "ms");
                }
                return false;
            }

            // If any snapshots are in progress, we don't want to apply any log entries with index greater than the snapshot's
            // index, because then we'd "lose" the ability to update the follower with that log entry, and as a result just have
            // to send a snapshot again. However, we impose a limit on how long we'll wait for a slow follower.
            for (Follower follower : this.followerMap.values()) {
                final SnapshotTransmit snapshotTransmit = follower.getSnapshotTransmit();
                if (snapshotTransmit == null)
                    continue;
                if (snapshotTransmit.getSnapshotIndex() < logEntry.getIndex()
                  && snapshotTransmit.getAge() < MAX_SNAPSHOT_TRANSMIT_AGE) {
                    if (this.log.isTraceEnabled()) {
                        this.trace("delaying application of " + logEntry + " because of in-progress snapshot install of "
                          + snapshotTransmit.getSnapshotIndex() + "t" + snapshotTransmit.getSnapshotTerm()
                          + " to " + follower);
                    }
                    return false;
                }
            }

            // If some follower does not yet have the log entry, wait for them to get it (up to some maximum time)
            if (logEntry.getAge() < MAX_SLOW_FOLLOWER_APPLY_DELAY_HEARTBEATS * this.raft.heartbeatTimeout) {
                for (Follower follower : this.followerMap.values()) {
                    if (follower.getMatchIndex() < logEntry.getIndex()) {
                        if (this.log.isTraceEnabled()) {
                            this.trace("delaying application of " + logEntry + " (age " + logEntry.getAge()
                              + " < " + (MAX_SLOW_FOLLOWER_APPLY_DELAY_HEARTBEATS * this.raft.heartbeatTimeout)
                              + ") because of slow " + follower);
                        }
                        return false;
                    }
                }
            }

            // OK
            return true;
        }

        /**
         * Update my {@code commitIndex} based on followers' {@code matchIndex}'s.
         *
         * <p>
         * This should be invoked:
         * <ul>
         *  <li>After any log entry has been added to the log, if we have zero followers</li>
         *  <li>After a log entry that contains a configuration change has been added to the log</li>
         *  <li>After a follower's {@linkplain Follower#getMatchIndex match index} has advanced</li>
         * </ul>
         */
        private void updateLeaderCommitIndex() {

            // Update my commit index based on when a majority of cluster members have ack'd log entries
            final long lastLogIndex = this.raft.getLastLogIndex();
            while (this.raft.commitIndex < lastLogIndex) {

                // Get the index in question
                final long index = this.raft.commitIndex + 1;

                // Count the number of nodes which have a copy of the log entry at index
                int numVotes = this.raft.isClusterMember() ? 1 : 0;                         // count myself, if member
                for (Follower follower : this.followerMap.values()) {                       // count followers who are members
                    if (follower.getMatchIndex() >= index && this.raft.isClusterMember(follower.getIdentity()))
                        numVotes++;
                }

                // Do a majority of cluster nodes have a copy of this log entry?
                final int majority = this.raft.currentConfig.size() / 2 + 1;
                if (numVotes < majority)
                    break;

                // Log entry term must match my current term
                final LogEntry logEntry = this.raft.getLogEntryAtIndex(index);
                if (logEntry.getTerm() != this.raft.currentTerm)
                    break;

                // Update commit index
                if (this.log.isDebugEnabled()) {
                    this.debug("advancing commit index from " + this.raft.commitIndex + " -> " + index
                      + " based on " + numVotes + "/" + this.raft.currentConfig.size() + " nodes having received " + logEntry);
                }
                this.raft.commitIndex = index;

                // Perform various service
                this.raft.requestService(this.checkReadyTransactionsService);
                this.raft.requestService(this.checkWaitingTransactionsService);
                this.raft.requestService(this.applyCommittedLogEntriesService);

                // Notify all (up-to-date) followers with the updated leaderCommit
                this.updateAllSynchronizedFollowersNow();
            }

            // If we are no longer a member of our cluster, step down as leader after the latest config change is committed
            if (!this.raft.isClusterMember()
              && this.raft.commitIndex >= this.findMostRecentConfigChangeMatching(Predicates.<String[]>alwaysTrue())) {
                this.log.info("stepping down as leader of cluster (no longer a member)");
                this.raft.changeRole(new FollowerRole(this.raft));
            }
        }

        /**
         * Update my {@code leaseTimeout} based on followers' returned {@code leaderTimeout}'s.
         *
         * <p>
         * This should be invoked:
         * <ul>
         *  <li>After a follower has replied with an {@link AppendResponse} containing a newer
         *      {@linkplain AppendResponse#getLeaderTimestamp leader timestamp} than before</li>
         * </ul>
         */
        private void updateLeaseTimeout() {

            // Only needed when we have followers
            final int numFollowers = this.followerMap.size();
            if (numFollowers == 0)
                return;

            // Get all cluster member leader timestamps, sorted in increasing order
            final Timestamp[] leaderTimestamps = new Timestamp[this.raft.currentConfig.size()];
            int index = 0;
            if (this.raft.isClusterMember())
                leaderTimestamps[index++] = new Timestamp();                                // only matters in single node cluster
            for (Follower follower : this.followerMap.values()) {
                if (this.raft.isClusterMember(follower.getIdentity()))
                    leaderTimestamps[index++] = follower.getLeaderTimestamp();
            }
            Preconditions.checkArgument(index == leaderTimestamps.length);                  // sanity check
            Arrays.sort(leaderTimestamps);

            //
            // Calculate highest leaderTimeout shared by a majority of cluster members, based on sorted array:
            //
            //  # nodes    timestamps
            //  -------    ----------
            //     5       [ ][ ][x][x][x]        3/5 x's make a majority at index (5 - 1)/2 = 2
            //     6       [ ][ ][x][x][x][x]     4/6 x's make a majority at index (6 - 1)/2 = 2
            //
            // The minimum leaderTimeout shared by a majority of nodes is at index (leaderTimestamps.length - 1) / 2.
            // We then add the minimum election timeout, then subtract a little for clock drift.
            //
            final Timestamp newLeaseTimeout = leaderTimestamps[(leaderTimestamps.length + 1) / 2]
              .offset((int)(this.raft.minElectionTimeout * (1.0f - MAX_CLOCK_DRIFT)));

            // Immediately notify any followers who are waiting on this updated timestamp (or any smaller timestamp)
            if (newLeaseTimeout.compareTo(this.leaseTimeout) > 0) {

                // Update my leader lease timeout
                if (this.log.isTraceEnabled())
                    this.trace("updating my lease timeout from " + this.leaseTimeout + " -> " + newLeaseTimeout);
                this.leaseTimeout = newLeaseTimeout;

                // Notify any followers who care
                for (Follower follower : this.followerMap.values()) {
                    final NavigableSet<Timestamp> timeouts = follower.getCommitLeaseTimeouts().headSet(this.leaseTimeout, true);
                    if (!timeouts.isEmpty()) {
                        follower.updateNow();                           // notify follower so it can commit waiting transaction(s)
                        timeouts.clear();
                    }
                }
            }
        }

        /**
         * Update our list of followers to match our current configuration.
         *
         * <p>
         * This should be invoked:
         * <ul>
         *  <li>After a log entry that contains a configuration change has been added to the log</li>
         *  <li>When the {@linkplain Follower#getNextIndex next index} of a follower not in the current config advances</li>
         * </ul>
         */
        private void updateKnownFollowers() {

            // Compare known followers with the current config and determine who needs to be be added or removed
            final HashSet<String> adds = new HashSet<>(this.raft.currentConfig.keySet());
            adds.removeAll(this.followerMap.keySet());
            adds.remove(this.raft.identity);
            final HashSet<String> dels = new HashSet<>(this.followerMap.keySet());
            dels.removeAll(this.raft.currentConfig.keySet());

            // Keep around a follower after its removal until it receives the config change that removed it
            for (Follower follower : this.followerMap.values()) {

                // Is this follower scheduled for deletion?
                final String peer = follower.getIdentity();
                if (!dels.contains(peer))
                    continue;

                // Find the most recent log entry containing a config change in which the follower was removed
                final String node = follower.getIdentity();
                final long index = this.findMostRecentConfigChangeMatching(new Predicate<String[]>() {
                    @Override
                    public boolean apply(String[] configChange) {
                        return configChange[0].equals(node) && configChange[1] == null;
                    }
                });

                // If follower has not received that log entry yet, keep on updating them until they do
                if (follower.getMatchIndex() < index)
                    dels.remove(peer);
            }

            // Add new followers
            for (String peer : adds) {
                final String address = this.raft.currentConfig.get(peer);
                final Follower follower = new Follower(peer, address, this.raft.getLastLogIndex());
                this.debug("adding new follower \"" + peer + "\" at " + address);
                follower.setUpdateTimer(
                  this.raft.new Timer("update timer for \"" + peer + "\"", new UpdateFollowerService(follower)));
                this.followerMap.put(peer, follower);
                follower.updateNow();                                               // schedule an immediate update
            }

            // Remove old followers
            for (String peer : dels) {
                final Follower follower = this.followerMap.remove(peer);
                this.debug("removing old follower \"" + peer + "\"");
                follower.cleanup();
            }
        }

        /**
         * Check whether a follower needs an update and send one if so.
         *
         * <p>
         * This should be invoked:
         * <ul>
         *  <li>After a new follower has been added</li>
         *  <li>When the output queue for a follower goes from non-empty to empty</li>
         *  <li>After the follower's {@linkplain Follower#getUpdateTimer update timer} has expired</li>
         *  <li>After a new log entry has been added to the log (all followers)</li>
         *  <li>After receiving an {@link AppendResponse} that caused the follower's
         *      {@linkplain Follower#getNextIndex next index} to change</li>
         *  <li>After receiving the first positive {@link AppendResponse} to a probe</li>
         *  <li>After our {@code commitIndex} has advanced (all followers)</li>
         *  <li>After our {@code leaseTimeout} has advanced past one or more of a follower's
         *      {@linkplain Follower#getCommitLeaseTimeouts commit lease timeouts} (with update timer reset)</li>
         *  <li>After sending a {@link CommitResponse} with a non-null {@linkplain CommitResponse#getCommitLeaderLeaseTimeout
         *      commit leader lease timeout} (all followers) to probe for updated leader timestamps</li>
         *  <li>After starting, aborting, or completing a snapshot install for a follower</li>
         * </ul>
         */
        private void updateFollower(Follower follower) {

            // If follower has an in-progress snapshot that has become too stale, abort it
            final String peer = follower.getIdentity();
            SnapshotTransmit snapshotTransmit = follower.getSnapshotTransmit();
            if (snapshotTransmit != null && snapshotTransmit.getSnapshotIndex() < this.raft.lastAppliedIndex) {
                if (this.log.isDebugEnabled())
                    this.debug("aborting stale snapshot install for " + follower);
                follower.cancelSnapshotTransmit();
                follower.updateNow();
            }

            // Is follower's queue empty? If not, hold off until then
            if (this.raft.isTransmitting(follower.getAddress())) {
                if (this.log.isTraceEnabled())
                    this.trace("no update for \"" + peer + "\": output queue still not empty");
                return;
            }

            // Handle any in-progress snapshot install
            if ((snapshotTransmit = follower.getSnapshotTransmit()) != null) {

                // Send the next chunk in transmission, if any
                final long pairIndex = snapshotTransmit.getPairIndex();
                final ByteBuffer chunk = snapshotTransmit.getNextChunk();
                boolean synced = true;
                if (chunk != null) {

                    // Send next chunk
                    final InstallSnapshot msg = new InstallSnapshot(this.raft.clusterId, this.raft.identity, peer,
                      this.raft.currentTerm, snapshotTransmit.getSnapshotTerm(), snapshotTransmit.getSnapshotIndex(), pairIndex,
                      pairIndex == 0 ? snapshotTransmit.getSnapshotConfig() : null, !snapshotTransmit.hasMoreChunks(), chunk);
                    if (this.raft.sendMessage(msg))
                        return;
                    if (this.log.isDebugEnabled())
                        this.debug("canceling snapshot install for " + follower + " due to failure to send " + msg);

                    // Message failed -> snapshot is fatally wounded, so cancel it
                    synced = false;
                }
                if (synced)
                    this.info("completed snapshot install for out-of-date " + follower);

                // Snapshot transmit is complete (or failed)
                follower.cancelSnapshotTransmit();

                // Trigger an immediate regular update
                follower.setNextIndex(snapshotTransmit.getSnapshotIndex() + 1);
                follower.setSynced(synced);
                follower.updateNow();
                this.raft.requestService(new UpdateFollowerService(follower));
                return;
            }

            // Are we still waiting for the update timer to expire?
            if (!follower.getUpdateTimer().pollForTimeout()) {
                boolean waitForTimerToExpire = true;

                // Don't wait for the update timer to expire if:
                //  (a) The follower is sync'd; AND
                //      (y) We have a new log entry that the follower doesn't have; OR
                //      (y) We have a new leaderCommit that the follower doesn't have
                // The effect is that we will pipeline updates to synchronized followers.
                if (follower.isSynced()
                  && (follower.getLeaderCommit() != this.raft.commitIndex
                   || follower.getNextIndex() <= this.raft.getLastLogIndex()))
                    waitForTimerToExpire = false;

                // Wait for timer to expire
                if (waitForTimerToExpire) {
                    if (this.log.isTraceEnabled()) {
                        this.trace("no update for \"" + follower.getIdentity() + "\": timer not expired yet, and follower is "
                          + (follower.isSynced() ? "up to date" : "not synced"));
                    }
                    return;
                }
            }

            // Get index of the next log entry to send to follower
            final long nextIndex = follower.getNextIndex();

            // If follower is too far behind, we must do a snapshot install
            if (nextIndex <= this.raft.lastAppliedIndex) {
                final MostRecentView view = this.raft.new MostRecentView();
                follower.setSnapshotTransmit(new SnapshotTransmit(this.raft.getLastLogTerm(), this.raft.getLastLogIndex(),
                  this.raft.lastAppliedConfig, view.getSnapshot(), view.getView()));
                this.info("started snapshot install for out-of-date " + follower);
                this.raft.requestService(new UpdateFollowerService(follower));
                return;
            }

            // Restart update timer here (to avoid looping if an error occurs below)
            follower.getUpdateTimer().timeoutAfter(this.raft.heartbeatTimeout);

            // Send actual data if follower is synced and there is a log entry to send; otherwise, just send a probe
            final AppendRequest msg;
            if (!follower.isSynced() || nextIndex > this.raft.getLastLogIndex()) {
                msg = new AppendRequest(this.raft.clusterId, this.raft.identity, peer, this.raft.currentTerm, new Timestamp(),
                  this.leaseTimeout, this.raft.commitIndex, this.raft.getLogTermAtIndex(nextIndex - 1), nextIndex - 1);   // probe
            } else {

                // Get log entry to send
                final LogEntry logEntry = this.raft.getLogEntryAtIndex(nextIndex);

                // If the log entry correspond's to follower's transaction, don't send the data because follower already has it.
                // But only do this optimization the first time, in case something goes wrong on the follower's end.
                ByteBuffer mutationData = null;
                if (!follower.getSkipDataLogEntries().remove(logEntry)) {
                    try {
                        mutationData = logEntry.getContent();
                    } catch (IOException e) {
                        this.error("error reading log file " + logEntry.getFile(), e);
                        return;
                    }
                }

                // Create message
                msg = new AppendRequest(this.raft.clusterId, this.raft.identity, peer, this.raft.currentTerm, new Timestamp(),
                  this.leaseTimeout, this.raft.commitIndex, this.raft.getLogTermAtIndex(nextIndex - 1), nextIndex - 1,
                  logEntry.getTerm(), mutationData);
            }

            // Send update
            final boolean sent = this.raft.sendMessage(msg);

            // Advance next index if a log entry was sent; we allow pipelining log entries when synchronized
            if (sent && !msg.isProbe()) {
                assert follower.isSynced();
                follower.setNextIndex(Math.min(follower.getNextIndex(), this.raft.getLastLogIndex()) + 1);
            }

            // Update leaderCommit for follower
            if (sent)
                follower.setLeaderCommit(msg.getLeaderCommit());
        }

        private void updateAllSynchronizedFollowersNow() {
            for (Follower follower : this.followerMap.values()) {
                if (follower.isSynced())
                    follower.updateNow();
            }
        }

        private class UpdateFollowerService extends Service {

            private final Follower follower;

            UpdateFollowerService(Follower follower) {
                super(LeaderRole.this, "update follower \"" + follower.getIdentity() + "\"");
                assert follower != null;
                this.follower = follower;
            }

            @Override
            public void run() {
                LeaderRole.this.updateFollower(this.follower);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null || obj.getClass() != this.getClass())
                    return false;
                final UpdateFollowerService that = (UpdateFollowerService)obj;
                return this.follower.equals(that.follower);
            }

            @Override
            public int hashCode() {
                return this.follower.hashCode();
            }
        }

    // Transactions

        @Override
        public void checkReadyTransaction(final RaftKVTransaction tx) {

            // Sanity check
            assert Thread.holdsLock(this.raft);
            assert tx.getState().equals(TxState.COMMIT_READY);

            // Check snapshot isolation
            if (this.checkReadyTransactionReadOnlySnapshot(tx))
                return;

            // Check for conflict
            final String error = this.checkConflicts(tx.getBaseTerm(), tx.getBaseIndex(), tx.getMutableView().getReads());
            if (error != null) {
                if (this.log.isDebugEnabled())
                    this.debug("local transaction " + tx + " failed due to conflict: " + error);
                throw new RetryTransactionException(tx, error);
            }

            // Handle read-only vs. read-write transaction
            final Writes writes = tx.getMutableView().getWrites();
            if (writes.isEmpty() && tx.getConfigChange() == null) {

                // Set commit term and index from last log entry
                tx.setCommitTerm(this.raft.getLastLogTerm());
                tx.setCommitIndex(this.raft.getLastLogIndex());
                if (this.log.isDebugEnabled()) {
                    this.debug("commit is " + tx.getCommitIndex() + "t" + tx.getCommitTerm()
                      + " for local read-only transaction " + tx);
                }

                // We will be able to commit this transaction immediately
                this.raft.requestService(new CheckWaitingTransactionService(tx));
            } else {

                // Don't commit a new config change while there is any previous config change outstanding
                if (tx.getConfigChange() != null && this.isConfigChangeOutstanding())
                    return;

                // Commit transaction as a new log entry
                final LogEntry logEntry;
                try {
                    logEntry = this.applyNewLogEntry(this.raft.new NewLogEntry(tx));
                } catch (Exception e) {
                    throw new KVTransactionException(tx, "error attempting to persist transaction", e);
                }
                if (this.log.isDebugEnabled())
                    this.debug("added log entry " + logEntry + " for local transaction " + tx);

                // Set commit term and index from new log entry
                tx.setCommitTerm(logEntry.getTerm());
                tx.setCommitIndex(logEntry.getIndex());

                // If there are no followers, we can commit this immediately
                if (this.followerMap.isEmpty())
                    this.raft.requestService(new CheckWaitingTransactionService(tx));
            }

            // Update transaction state
            tx.setState(TxState.COMMIT_WAITING);
            tx.getMutableView().disableReadTracking();                                  // we no longer need this info
        }

        @Override
        public void cleanupForTransaction(RaftKVTransaction tx) {
            // nothing to do
        }

        private boolean isConfigChangeOutstanding() {
            for (int i = (int)(this.raft.commitIndex - this.raft.lastAppliedIndex) + 1; i < this.raft.raftLog.size(); i++) {
                if (this.raft.raftLog.get(i).getConfigChange() != null)
                    return true;
            }
            return false;
        }

    // Message

        @Override
        public void caseAppendRequest(AppendRequest msg) {
            this.failDuplicateLeader(msg);
        }

        @Override
        public void caseAppendResponse(AppendResponse msg) {

            // Find follower
            final Follower follower = this.findFollower(msg);
            if (follower == null)
                return;

            // Update follower's last rec'd leader timestamp
            if (msg.getLeaderTimestamp().compareTo(follower.getLeaderTimestamp()) > 0) {
                follower.setLeaderTimestamp(msg.getLeaderTimestamp());
                this.raft.requestService(this.updateLeaseTimeoutService);
            }

            // Ignore if a snapshot install is in progress
            if (follower.getSnapshotTransmit() != null) {
                if (this.log.isTraceEnabled())
                    this.trace("rec'd " + msg + " while sending snapshot install; ignoring");
                return;
            }

            // Flag indicating we might want to update follower when done
            boolean updateFollowerAgain = false;

            // Update follower's match index
            if (msg.getMatchIndex() > follower.getMatchIndex()) {
                follower.setMatchIndex(msg.getMatchIndex());
                this.raft.requestService(this.updateLeaderCommitIndexService);
                this.raft.requestService(this.applyCommittedLogEntriesService);
                if (!this.raft.isClusterMember(follower.getIdentity()))
                    this.raft.requestService(this.updateKnownFollowersService);
            }

            // Check result and update follower's next index
            final boolean wasSynced = follower.isSynced();
            final long previousNextIndex = follower.getNextIndex();
            if (!msg.isSuccess())
                follower.setNextIndex(Math.max(follower.getNextIndex() - 1, 1));
            follower.setSynced(msg.isSuccess());
            if (follower.isSynced() != wasSynced) {
                if (this.log.isDebugEnabled()) {
                    this.debug("sync status of \"" + follower.getIdentity() + "\" changed -> "
                      + (!follower.isSynced() ? "not " : "") + "synced");
                }
                updateFollowerAgain = true;
            }

            // Use follower's match index as a lower bound on follower's next index. This is needed because in this implementation,
            // the application of leader log entries (to the state machine) may occur later than the application of follower log
            // entries, so it's possible a follower's state machine's index can advance past the leader's. In that case we want
            // to avoid the leader sending log entries that are prior to the follower's last committed index.
            follower.setNextIndex(Math.max(follower.getNextIndex(), follower.getMatchIndex() + 1));

            // Use follower's last log index as an upper bound on follower's next index.
            follower.setNextIndex(Math.min(msg.getLastLogIndex() + 1, follower.getNextIndex()));

            // Update follower again if next index has changed
            updateFollowerAgain |= follower.getNextIndex() != previousNextIndex;

            // Debug
            if (this.log.isTraceEnabled())
                this.trace("updated follower: " + follower + ", update again = " + updateFollowerAgain);

            // Immediately update follower again (if appropriate)
            if (updateFollowerAgain)
                this.raft.requestService(new UpdateFollowerService(follower));
        }

        @Override
        public void caseCommitRequest(CommitRequest msg) {

            // Find follower
            final Follower follower = this.findFollower(msg);
            if (follower == null)
                return;

            // Decode reads
            final Reads reads;
            try {
                reads = Reads.deserialize(new ByteBufferInputStream(msg.getReadsData()));
            } catch (Exception e) {
                this.error("error decoding reads data in " + msg, e);
                this.raft.sendMessage(new CommitResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getTxId(), "error decoding reads data: " + e));
                return;
            }

            // Check for conflict
            final String conflictMsg = this.checkConflicts(msg.getBaseTerm(), msg.getBaseIndex(), reads);
            if (conflictMsg != null) {
                if (this.log.isDebugEnabled())
                    this.debug("commit request " + msg + " failed due to conflict: " + conflictMsg);
                this.raft.sendMessage(new CommitResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getTxId(), conflictMsg));
                return;
            }

            // Handle read-only vs. read-write transaction
            if (msg.isReadOnly()) {

                // Get current time
                final Timestamp minimumLeaseTimeout = new Timestamp();

                // The follower may commit as soon as it sees the transaction's BASE log entry get committed.
                // Note, we don't need to wait for any subsequent log entries to be committed, because if they
                // are committed they are invisible to the transaction, and if they aren't ever committed then
                // whatever log entries replace them will necessarily have been created sometime after now.
                final CommitResponse response;
                if (this.leaseTimeout.compareTo(minimumLeaseTimeout) > 0) {

                    // No other leader could have been elected yet as of right now, so the transaction can commit immediately
                    response = new CommitResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                      this.raft.currentTerm, msg.getTxId(), msg.getBaseTerm(), msg.getBaseIndex());
                } else {

                    // Remember that this follower is now going to be waiting for this particular leaseTimeout
                    follower.getCommitLeaseTimeouts().add(minimumLeaseTimeout);

                    // Send immediate probes to all (up-to-date) followers in an attempt to increase our leaseTimeout quickly
                    this.updateAllSynchronizedFollowersNow();

                    // Build response
                    response = new CommitResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                      this.raft.currentTerm, msg.getTxId(), msg.getBaseTerm(), msg.getBaseIndex(), minimumLeaseTimeout);
                }

                // Send response
                this.raft.sendMessage(response);
            } else {

                // If the client is requesting a config change, we could check for an outstanding config change now and if so
                // delay our response until it completes, but that's not worth the trouble. Instead, applyNewLogEntry() will
                // throw an exception and the client will just just have to retry the transaction.

                // Commit mutations as a new log entry
                final LogEntry logEntry;
                try {
                    logEntry = this.applyNewLogEntry(this.raft.new NewLogEntry(msg.getMutationData()));
                } catch (Exception e) {
                    this.error("error appending new log entry for " + msg, e);
                    this.raft.sendMessage(new CommitResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                      this.raft.currentTerm, msg.getTxId(), e.getMessage() != null ? e.getMessage() : "" + e));
                    return;
                }
                if (this.log.isDebugEnabled())
                    this.debug("added log entry " + logEntry + " for remote " + msg);

                // Follower transaction data optimization
                follower.getSkipDataLogEntries().add(logEntry);

                // Send response
                this.raft.sendMessage(new CommitResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getTxId(), logEntry.getTerm(), logEntry.getIndex()));
            }
        }

        @Override
        public void caseCommitResponse(CommitResponse msg) {
            this.failDuplicateLeader(msg);
        }

        @Override
        public void caseInstallSnapshot(InstallSnapshot msg) {
            this.failDuplicateLeader(msg);
        }

        @Override
        public void caseRequestVote(RequestVote msg) {

            // Too late dude, I already won the election
            if (this.log.isDebugEnabled())
                this.debug("ignoring " + msg + " rec'd while in " + this);
        }

        @Override
        public void caseGrantVote(GrantVote msg) {

            // Thanks and all, but I already won the election
            if (this.log.isDebugEnabled())
                this.debug("ignoring " + msg + " rec'd while in " + this);
        }

        private void failDuplicateLeader(Message msg) {

            // This should never happen - same term but two different leaders
            final boolean defer = this.raft.identity.compareTo(msg.getSenderId()) <= 0;
            this.error("detected a duplicate leader in " + msg + " - should never happen; possible inconsistent cluster"
              + " configuration on " + msg.getSenderId() + " (mine: " + this.raft.currentConfig + "); "
              + (defer ? "reverting to follower" : "ignoring"));
            if (defer)
                this.raft.changeRole(new FollowerRole(this.raft, msg.getSenderId(), this.raft.returnAddress));
        }

    // Object

        @Override
        public String toString() {
            return this.toStringPrefix()
              + ",followerMap=" + this.followerMap
              + "]";
        }

    // Debug

        @Override
        boolean checkState() {
            if (!super.checkState())
                return false;
            for (Follower follower : this.followerMap.values()) {
                assert follower.getNextIndex() <= this.raft.getLastLogIndex() + 1;
                assert follower.getMatchIndex() <= this.raft.getLastLogIndex() + 1;
                assert follower.getLeaderCommit() <= this.raft.commitIndex;
                assert follower.getUpdateTimer().isRunning() || follower.getSnapshotTransmit() != null;
            }
            return true;
        }

    // Internal methods

        /**
         * Find the index of the most recent unapplied log entry having an associated config change matching the given predicate.
         *
         * @return most recent matching log entry, or zero if none found
         */
        private long findMostRecentConfigChangeMatching(Predicate<String[]> predicate) {
            for (long index = this.raft.getLastLogIndex(); index > this.raft.lastAppliedIndex; index--) {
                final String[] configChange = this.raft.getLogEntryAtIndex(index).getConfigChange();
                if (configChange == null && predicate.apply(configChange))
                    return index;
            }
            return 0;
        }

        // Apply a new log entry to the Raft log
        private LogEntry applyNewLogEntry(NewLogEntry newLogEntry) throws Exception {

            // Do a couple of extra checks if a config change is included
            final String[] configChange = newLogEntry.getData().getConfigChange();
            if (configChange != null) {

                // Disallow a config change while there is a previous uncommitted config change
                if (this.isConfigChangeOutstanding()) {
                    newLogEntry.cancel();
                    throw new IllegalStateException("uncommitted config change outstanding");
                }

                // Disallow a configuration change that removes the last node in a cluster
                if (this.raft.currentConfig.size() == 1 && configChange[1] == null) {
                    final String lastNode = this.raft.currentConfig.keySet().iterator().next();
                    if (configChange[0].equals(lastNode)) {
                        newLogEntry.cancel();
                        throw new IllegalArgumentException("can't remove the last node in a cluster (\"" + lastNode + "\")");
                    }
                }
            }

            // Append a new entry to the Raft log
            final LogEntry logEntry;
            boolean success = false;
            try {
                logEntry = this.raft.appendLogEntry(this.raft.currentTerm, newLogEntry);
                success = true;
            } finally {
                if (!success)
                    newLogEntry.cancel();
            }

            // Update follower list if configuration changed
            if (configChange != null)
                this.raft.requestService(this.updateKnownFollowersService);

            // Update memory usage
            this.totalLogEntryMemoryUsed += logEntry.getFileSize();

            // Update commit index (this is only needed if config has changed, or in the single node case)
            if (configChange != null || this.followerMap.isEmpty())
                this.raft.requestService(this.updateLeaderCommitIndexService);

            // Immediately update all up-to-date followers
            this.updateAllSynchronizedFollowersNow();

            // Done
            return logEntry;
        }

        /**
         * Check whether a proposed transaction can commit without any MVCC conflict.
         *
         * @param file file containing serialized copy of {@link writes} (content must already be fsync()'d to disk!)
         * @param baseTerm the term of the log entry on which the transaction is based
         * @param baseIndex the index of the log entry on which the transaction is based
         * @param reads reads performed by the transaction
         * @param writes writes performed by the transaction
         * @return error message on failure, null for success
         */
        public String checkConflicts(long baseTerm, long baseIndex, Reads reads) {

            // Validate the index of the log entry on which the transaction is based
            final long minIndex = this.raft.lastAppliedIndex;
            final long maxIndex = this.raft.getLastLogIndex();
            if (baseIndex < minIndex)
                return "transaction is too old: snapshot index " + baseIndex + " < last applied log index " + minIndex;
            if (baseIndex > maxIndex)
                return "transaction is too new: snapshot index " + baseIndex + " > most recent log index " + maxIndex;

            // Validate the term of the log entry on which the transaction is based
            final long actualBaseTerm = this.raft.getLogTermAtIndex(baseIndex);
            if (baseTerm != actualBaseTerm) {
                return "transaction is based on an overwritten log entry with index "
                  + baseIndex + " and term " + baseTerm + " != " + actualBaseTerm;
            }

            // Check for conflicts from intervening commits
            for (long index = baseIndex + 1; index <= maxIndex; index++) {
                final LogEntry logEntry = this.raft.getLogEntryAtIndex(index);
                if (reads.isConflict(logEntry.getWrites())) {
                    return "writes of committed transaction at index " + index
                      + " conflict with transaction reads from transaction base index " + baseIndex;
                }
            }

            // No conflict
            return null;
        }

        private Follower findFollower(Message msg) {
            final Follower follower = this.followerMap.get(msg.getSenderId());
            if (follower == null)
                this.warn("rec'd " + msg + " from unknown follower \"" + msg.getSenderId() + "\", ignoring");
            return follower;
        }
    }

    /**
     * Support superclass for {@link FollowerRole} and {@link CandidateRole}, which both have an election timer.
     */
    private abstract static class NonLeaderRole extends Role {

        protected final Timer electionTimer = this.raft.new Timer("election timer", new Service(this, "election timeout") {
            @Override
            public void run() {
                NonLeaderRole.this.checkElectionTimeout();
            }
        });
        private final boolean startElectionTimer;

    // Constructors

        protected NonLeaderRole(RaftKVDatabase raft, boolean startElectionTimer) {
            super(raft);
            this.startElectionTimer = startElectionTimer;
        }

    // Lifecycle

        @Override
        public void setup() {
            super.setup();
            if (this.startElectionTimer)
                this.restartElectionTimer();
        }

        @Override
        public void shutdown() {
            super.shutdown();
            this.electionTimer.cancel();
        }

    // Service

        // Check for an election timeout
        private void checkElectionTimeout() {
            if (this.electionTimer.pollForTimeout()) {
                this.info("election timeout while in " + this);
                this.raft.changeRole(new CandidateRole(this.raft));
            }
        }

        protected void restartElectionTimer() {

            // Sanity check
            assert Thread.holdsLock(this.raft);

            // Generate a randomized election timeout delay
            final int range = this.raft.maxElectionTimeout - this.raft.minElectionTimeout;
            final int randomizedPart = Math.round(this.raft.random.nextFloat() * range);

            // Restart timer
            this.electionTimer.timeoutAfter(this.raft.minElectionTimeout + randomizedPart);
        }

    // MessageSwitch

        @Override
        public void caseAppendResponse(AppendResponse msg) {
            this.failUnexpectedMessage(msg);
        }

        @Override
        public void caseCommitRequest(CommitRequest msg) {
            this.failUnexpectedMessage(msg);
        }
    }

// FOLLOWER role

    private static class FollowerRole extends NonLeaderRole {

        private String leader;                                                          // our leader, if known
        private String leaderAddress;                                                   // our leader's network address
        private String votedFor;                                                        // the candidate we voted for this term
        private SnapshotReceive snapshotReceive;                                        // in-progress snapshot install, if any
        private final HashMap<Long, PendingRequest> pendingRequests = new HashMap<>();  // wait for CommitResponse or log entry
        private final HashMap<Long, PendingWrite> pendingWrites = new HashMap<>();      // wait for AppendRequest with null data
        private final HashMap<Long, Timestamp> commitLeaderLeaseTimeoutMap              // tx's waiting for leaderLeaseTimeout's
          = new HashMap<>();
        private Timestamp lastLeaderMessageTime;                                        // time of most recent rec'd AppendRequest
        private Timestamp leaderLeaseTimeout;                                           // latest rec'd leader lease timeout

    // Constructors

        public FollowerRole(RaftKVDatabase raft) {
            this(raft, null, null, null);
        }

        public FollowerRole(RaftKVDatabase raft, String leader, String leaderAddress) {
            this(raft, leader, leaderAddress, leader);
        }

        public FollowerRole(RaftKVDatabase raft, String leader, String leaderAddress, String votedFor) {
            super(raft, raft.isClusterMember());
            this.leader = leader;
            this.leaderAddress = leaderAddress;
            this.votedFor = votedFor;
            assert this.leaderAddress != null || this.leader == null;
        }

    // Lifecycle

        @Override
        public void setup() {
            super.setup();
            this.info("entering follower role in term " + this.raft.currentTerm
              + (this.leader != null ? "; with leader \"" + this.leader + "\" at " + this.leaderAddress : "")
              + (this.votedFor != null ? "; having voted for \"" + this.votedFor + "\"" : ""));
        }

        @Override
        public void shutdown() {
            super.shutdown();

            // Cancel any in-progress snapshot install
            if (this.snapshotReceive != null) {
                if (this.log.isDebugEnabled())
                    this.debug("aborting snapshot install due to leaving follower role");
                this.raft.resetStateMachine(true);
                this.snapshotReceive = null;
            }

            // Fail any (read-only) transactions waiting on a minimum lease timeout from deposed leader
            for (RaftKVTransaction tx : new ArrayList<RaftKVTransaction>(this.raft.openTransactions.values())) {
                if (tx.getState().equals(TxState.COMMIT_WAITING) && this.commitLeaderLeaseTimeoutMap.containsKey(tx.getTxId()))
                    this.raft.fail(tx, new RetryTransactionException(tx, "leader was deposed during commit"));
            }

            // Cleanup pending requests and commit writes
            this.pendingRequests.clear();
            for (PendingWrite pendingWrite : this.pendingWrites.values())
                pendingWrite.cleanup();
            this.pendingWrites.clear();
        }

    // Service

        @Override
        public void outputQueueEmpty(String address) {
            if (address.equals(this.leaderAddress))
                this.raft.requestService(this.checkReadyTransactionsService);       // TODO: track specific transactions
        }

        // Check whether the required minimum leader lease timeout has been seen, if any
        @Override
        protected boolean mayCommit(RaftKVTransaction tx) {

            // Is there a required minimum leader lease timeout associated with the transaction?
            final Timestamp commitLeaderLeaseTimeout = this.commitLeaderLeaseTimeoutMap.get(tx.getTxId());
            if (commitLeaderLeaseTimeout == null)
                return true;

            // Do we know the leader's lease timeout yet?
            if (this.leaderLeaseTimeout == null)
                return false;

            // Verify leader's lease timeout has extended beyond that required by the transaction
            return this.leaderLeaseTimeout.compareTo(commitLeaderLeaseTimeout) >= 0;
        }

        /**
         * Check whether the election timer should be running, and make it so.
         *
         * <p>
         * This should be invoked:
         * <ul>
         *  <li>After a log entry that contains a configuration change has been added to the log</li>
         *  <li>When a snapshot install starts</li>
         *  <li>When a snapshot install completes</li>
         * </ul>
         */
        private void updateElectionTimer() {
            final boolean isClusterMember = this.raft.isClusterMember();
            final boolean electionTimerRunning = this.electionTimer.isRunning();
            if (isClusterMember && !electionTimerRunning) {
                if (this.log.isTraceEnabled())
                    this.trace("starting up election timer because I'm now part of the current config");
                this.restartElectionTimer();
            } else if (!isClusterMember && electionTimerRunning) {
                if (this.log.isTraceEnabled())
                    this.trace("stopping election timer because I'm no longer part of the current config");
                this.electionTimer.cancel();
            }
        }

    // Transactions

        @Override
        public void checkReadyTransaction(RaftKVTransaction tx) {

            // Sanity check
            assert Thread.holdsLock(this.raft);
            assert tx.getState().equals(TxState.COMMIT_READY);

            // Check snapshot isolation
            if (this.checkReadyTransactionReadOnlySnapshot(tx))
                return;

            // Did we already send a CommitRequest for this transaction?
            PendingRequest pendingRequest = this.pendingRequests.get(tx.getTxId());
            if (pendingRequest != null) {
                if (this.log.isTraceEnabled())
                    this.trace("leaving alone ready tx " + tx + " because request already sent");
                return;
            }

            // If we are installing a snapshot, we must wait
            if (this.snapshotReceive != null) {
                if (this.log.isTraceEnabled())
                    this.trace("leaving alone ready tx " + tx + " because a snapshot install is in progress");
                return;
            }

            // Handle situation where we are unconfigured and not part of any cluster yet
            if (!this.raft.isConfigured()) {

                // Get transaction mutations
                final Writes writes = tx.getMutableView().getWrites();
                final String[] configChange = tx.getConfigChange();

                // Allow an empty read-only transaction when unconfigured
                if (writes.isEmpty() && configChange == null) {
                    this.raft.succeed(tx);
                    return;
                }

                // Otherwise, we can only handle an initial config change that is adding the local node
                if (configChange == null || !configChange[0].equals(this.raft.identity) || configChange[1] == null) {
                    throw new RetryTransactionException(tx, "unconfigured system: an initial configuration change adding"
                      + " the local node (\"" + this.raft.identity + "\") as the first member of a new cluster is required");
                }

                // Create a new cluster if needed
                if (this.raft.clusterId == 0) {
                    this.info("creating new cluster");
                    if (!this.raft.joinCluster(0))
                        throw new KVTransactionException(tx, "error persisting new cluster ID");
                }

                // Advance term
                assert this.raft.currentTerm == 0;
                if (!this.raft.advanceTerm(this.raft.currentTerm + 1))
                    throw new KVTransactionException(tx, "error advancing term");

                // Append the first entry to the Raft log
                final LogEntry logEntry;
                try {
                    logEntry = this.raft.appendLogEntry(this.raft.currentTerm, this.raft.new NewLogEntry(tx));
                } catch (Exception e) {
                    throw new KVTransactionException(tx, "error attempting to persist transaction", e);
                }
                if (this.log.isDebugEnabled())
                    this.debug("added log entry " + logEntry + " for local transaction " + tx);
                assert logEntry.getTerm() == 1;
                assert logEntry.getIndex() == 1;

                // Set commit term and index from new log entry
                tx.setCommitTerm(logEntry.getTerm());
                tx.setCommitIndex(logEntry.getIndex());
                this.raft.commitIndex = logEntry.getIndex();

                // Update transaction state
                tx.setState(TxState.COMMIT_WAITING);
                tx.getMutableView().disableReadTracking();                                  // we no longer need this info

                // Immediately become the leader of our new single-node cluster
                assert this.raft.isConfigured();
                this.info("appointing myself leader in newly created cluster");
                this.raft.changeRole(new LeaderRole(this.raft));
                return;
            }

            // If we don't have a leader yet, or leader's queue is full, we must wait
            if (this.leader == null || this.raft.isTransmitting(this.leaderAddress)) {
                if (this.log.isTraceEnabled()) {
                    this.trace("leaving alone ready tx " + tx + " because leader "
                      + (this.leader == null ? "is not known yet" : "\"" + this.leader + "\" is not writable yet"));
                }
                return;
            }

            // Serialize reads into buffer
            final Reads reads = tx.getMutableView().getReads();
            final long readsDataSize = reads.serializedLength();
            if (readsDataSize != (int)readsDataSize)
                throw new KVTransactionException(tx, "transaction read information exceeds maximum length");
            final ByteBuffer readsData = Util.allocateByteBuffer((int)readsDataSize);
            try (ByteBufferOutputStream output = new ByteBufferOutputStream(readsData)) {
                reads.serialize(output);
            } catch (IOException e) {
                throw new RuntimeException("unexpected exception", e);
            }
            assert !readsData.hasRemaining();
            readsData.flip();

            // Handle read-only vs. read-write transaction
            final Writes writes = tx.getMutableView().getWrites();
            FileWriter fileWriter = null;
            ByteBuffer mutationData = null;
            if (!writes.isEmpty() || tx.getConfigChange() != null) {

                // Serialize changes into a temporary file (but do not close or durably persist yet)
                final File file = new File(this.raft.logDir,
                  String.format("%s%019d%s", TX_FILE_PREFIX, tx.getTxId(), TEMP_FILE_SUFFIX));
                try {
                    LogEntry.writeData((fileWriter = new FileWriter(file)), new LogEntry.Data(writes, tx.getConfigChange()));
                    fileWriter.flush();
                } catch (IOException e) {
                    fileWriter.getFile().delete();
                    Util.closeIfPossible(fileWriter);
                    throw new KVTransactionException(tx, "error saving transaction mutations to temporary file", e);
                }
                final long writeLength = fileWriter.getLength();

                // Load serialized writes from file
                try {
                    mutationData = Util.readFile(fileWriter.getFile(), writeLength);
                } catch (IOException e) {
                    fileWriter.getFile().delete();
                    Util.closeIfPossible(fileWriter);
                    throw new KVTransactionException(tx, "error reading transaction mutations from temporary file", e);
                }

                // Record pending commit write with temporary file
                final PendingWrite pendingWrite = new PendingWrite(tx, fileWriter);
                this.pendingWrites.put(tx.getTxId(), pendingWrite);
            }

            // Record pending request
            this.pendingRequests.put(tx.getTxId(), new PendingRequest(tx));

            // Send commit request to leader
            final CommitRequest msg = new CommitRequest(this.raft.clusterId, this.raft.identity, this.leader,
              this.raft.currentTerm, tx.getTxId(), tx.getBaseTerm(), tx.getBaseIndex(), readsData, mutationData);
            if (this.log.isTraceEnabled())
                this.trace("sending " + msg + " to \"" + this.leader + "\" for " + tx);
            if (!this.raft.sendMessage(msg))
                throw new RetryTransactionException(tx, "error sending commit request to leader");
        }

        @Override
        public void cleanupForTransaction(RaftKVTransaction tx) {
            this.pendingRequests.remove(tx.getTxId());
            final PendingWrite pendingWrite = this.pendingWrites.remove(tx.getTxId());
            if (pendingWrite != null)
                pendingWrite.cleanup();
            this.commitLeaderLeaseTimeoutMap.remove(tx.getTxId());
        }

    // Messages

        @Override
        public boolean mayAdvanceCurrentTerm(Message msg) {

            // Deny vote if we have heard from our leader within the minimum election timeout (dissertation, section 4.2.3)
            if (msg instanceof RequestVote
              && this.lastLeaderMessageTime != null
              && this.lastLeaderMessageTime.offsetFromNow() > -this.raft.minElectionTimeout)
                return false;

            // OK
            return true;
        }

        @Override
        public void caseAppendRequest(AppendRequest msg) {

            // Record new cluster ID if we haven't done so already
            if (this.raft.clusterId == 0)
                this.raft.joinCluster(msg.getClusterId());

            // Record leader
            if (!msg.getSenderId().equals(this.leader)) {
                if (this.leader != null && !this.leader.equals(msg.getSenderId())) {
                    this.error("detected a conflicting leader in " + msg + " (previous leader was \"" + this.leader
                      + "\") - should never happen; possible inconsistent cluster configuration (mine: " + this.raft.currentConfig
                      + ")");
                }
                this.leader = msg.getSenderId();
                this.leaderAddress = this.raft.returnAddress;
                this.leaderLeaseTimeout = msg.getLeaderLeaseTimeout();
                this.info("updated leader to \"" + this.leader + "\" at " + this.leaderAddress);
                this.raft.requestService(this.checkReadyTransactionsService);     // allows COMMIT_READY transactions to be sent
            }

            // Get message info
            final long leaderCommitIndex = msg.getLeaderCommit();
            final long leaderPrevTerm = msg.getPrevLogTerm();
            final long leaderPrevIndex = msg.getPrevLogIndex();
            final long logTerm = msg.getLogEntryTerm();
            final long logIndex = leaderPrevIndex + 1;

            // Update timestamp last heard from leader
            this.lastLeaderMessageTime = new Timestamp();

            // Update leader's lease timeout
            if (this.leaderLeaseTimeout == null || msg.getLeaderLeaseTimeout().compareTo(this.leaderLeaseTimeout) > 0) {
                if (this.log.isTraceEnabled())
                    this.trace("advancing leader lease timeout " + this.leaderLeaseTimeout + " -> " + msg.getLeaderLeaseTimeout());
                this.leaderLeaseTimeout = msg.getLeaderLeaseTimeout();
                this.raft.requestService(this.checkWaitingTransactionsService);
            }

            // If a snapshot install is in progress, cancel it
            if (this.snapshotReceive != null) {
                this.info("rec'd " + msg + " during in-progress " + this.snapshotReceive
                  + "; aborting snapshot install and resetting state machine");
                if (!this.raft.resetStateMachine(true))
                    return;
                this.snapshotReceive = null;
                this.updateElectionTimer();
            }

            // Restart election timeout (if running)
            if (this.electionTimer.isRunning())
                this.restartElectionTimer();

            // Get my last log entry's index and term
            long lastLogTerm = this.raft.getLastLogTerm();
            long lastLogIndex = this.raft.getLastLogIndex();

            // Check whether our previous log entry term matches that of leader; if not, or it doesn't exist, request fails
            if (leaderPrevIndex < this.raft.lastAppliedIndex
              || leaderPrevIndex > lastLogIndex
              || leaderPrevTerm != this.raft.getLogTermAtIndex(leaderPrevIndex)) {
                if (this.log.isDebugEnabled())
                    this.debug("rejecting " + msg + " because previous log entry doesn't match");
                this.raft.sendMessage(new AppendResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getLeaderTimestamp(), false, this.raft.lastAppliedIndex, this.raft.getLastLogIndex()));
                return;
            }

            // Check whether the message actually contains a log entry; if so, append it
            boolean success = true;
            if (!msg.isProbe()) {

                // Check for a conflicting (i.e., never committed, then overwritten) log entry that we need to clear away first
                if (logIndex <= lastLogIndex && this.raft.getLogTermAtIndex(logIndex) != msg.getLogEntryTerm()) {

                    // Delete conflicting log entry, and all entries that follow it, from the log
                    final int startListIndex = (int)(logIndex - this.raft.lastAppliedIndex - 1);
                    final List<LogEntry> conflictList = this.raft.raftLog.subList(startListIndex, this.raft.raftLog.size());
                    for (LogEntry logEntry : conflictList) {
                        this.info("deleting log entry " + logEntry + " overrwritten by " + msg);
                        if (!logEntry.getFile().delete())
                            this.error("failed to delete log file " + logEntry.getFile());
                    }
                    try {
                        this.raft.logDirChannel.force(true);
                    } catch (IOException e) {
                        this.warn("errory fsync()'ing log directory " + this.raft.logDir, e);
                    }
                    conflictList.clear();

                    // Rebuild current config
                    this.raft.currentConfig = this.raft.buildCurrentConfig();

                    // Update last log entry info
                    lastLogTerm = this.raft.getLastLogTerm();
                    lastLogIndex = this.raft.getLastLogIndex();
                }

                // Append the new log entry - if we don't already have it
                if (logIndex > lastLogIndex) {
                    assert logIndex == lastLogIndex + 1;
                    LogEntry logEntry = null;
                    do {

                        // If message contains no data, we expect to get the data from the corresponding transaction
                        final ByteBuffer mutationData = msg.getMutationData();
                        if (mutationData == null) {

                            // Find the matching pending commit write
                            final PendingWrite pendingWrite;
                            try {
                                pendingWrite = Iterables.find(this.pendingWrites.values(), new Predicate<PendingWrite>() {
                                    @Override
                                    public boolean apply(PendingWrite pendingWrite) {
                                        final RaftKVTransaction tx = pendingWrite.getTx();
                                        return tx.getState().equals(TxState.COMMIT_WAITING)
                                          && tx.getCommitTerm() == logTerm && tx.getCommitIndex() == logIndex;
                                    }
                                });
                            } catch (NoSuchElementException e) {
                                if (this.log.isDebugEnabled()) {
                                    this.debug("rec'd " + msg + " but no read-write transaction matching commit "
                                      + logIndex + "t" + logTerm + " found; rejecting");
                                }
                                break;
                            }

                            // Commit's writes are no longer pending
                            final RaftKVTransaction tx = pendingWrite.getTx();
                            this.pendingWrites.remove(tx.getTxId());

                            // Close and durably persist the associated temporary file
                            try {
                                pendingWrite.getFileWriter().close();
                            } catch (IOException e) {
                                this.error("error closing temporary transaction file for " + tx, e);
                                pendingWrite.cleanup();
                                break;
                            }

                            // Append a new log entry using temporary file
                            try {
                                logEntry = this.raft.appendLogEntry(logTerm,
                                  this.raft.new NewLogEntry(tx, pendingWrite.getFileWriter().getFile()));
                            } catch (Exception e) {
                                this.error("error appending new log entry for " + tx, e);
                                pendingWrite.cleanup();
                                break;
                            }

                            // Debug
                            if (this.log.isDebugEnabled()) {
                                this.debug("now waiting for commit of " + tx.getCommitIndex() + "t" + tx.getCommitTerm()
                                  + " to commit " + tx);
                            }
                        } else {

                            // Append new log entry normally using the data from the request
                            try {
                                logEntry = this.raft.appendLogEntry(logTerm, this.raft.new NewLogEntry(mutationData));
                            } catch (Exception e) {
                                this.error("error appending new log entry", e);
                                break;
                            }
                        }
                    } while (false);

                    // Start/stop election timer as needed
                    if (logEntry != null && logEntry.getConfigChange() != null)
                        this.updateElectionTimer();

                    // Success?
                    success = logEntry != null;

                    // Update last log entry info
                    lastLogTerm = this.raft.getLastLogTerm();
                    lastLogIndex = this.raft.getLastLogIndex();
                }
            }

            // Update my commit index
            final long newCommitIndex = Math.min(Math.max(leaderCommitIndex, this.raft.commitIndex), lastLogIndex);
            if (newCommitIndex > this.raft.commitIndex) {
                if (this.log.isDebugEnabled())
                    this.debug("updating leader commit index from " + this.raft.commitIndex + " -> " + newCommitIndex);
                this.raft.commitIndex = newCommitIndex;
                this.raft.requestService(this.checkWaitingTransactionsService);
                this.raft.requestService(this.applyCommittedLogEntriesService);
            }

            // Debug
            if (this.log.isTraceEnabled()) {
                this.trace("my updated follower state: "
                  + "term=" + this.raft.currentTerm
                  + " commitIndex=" + this.raft.commitIndex
                  + " leaderLeaseTimeout=" + this.leaderLeaseTimeout
                  + " lastApplied=" + this.raft.lastAppliedIndex + "t" + this.raft.lastAppliedTerm
                  + " log=" + this.raft.raftLog);
            }

            // Send reply
            if (success) {
                this.raft.sendMessage(new AppendResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getLeaderTimestamp(), true, msg.isProbe() ? logIndex - 1 : logIndex,
                  this.raft.getLastLogIndex()));
            } else {
                this.raft.sendMessage(new AppendResponse(this.raft.clusterId, this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getLeaderTimestamp(), false, this.raft.lastAppliedIndex, this.raft.getLastLogIndex()));
            }
        }

        @Override
        public void caseCommitResponse(CommitResponse msg) {

            // Find transaction
            final RaftKVTransaction tx = this.raft.openTransactions.get(msg.getTxId());
            if (tx == null)                                                                 // must have been rolled back locally
                return;

            // Sanity check transaction state
            if (!tx.getState().equals(TxState.COMMIT_READY)) {
                this.warn("rec'd " + msg + " for " + tx + " in state " + tx.getState() + "; ignoring");
                return;
            }
            if (this.pendingRequests.remove(tx.getTxId()) == null) {
                if (this.log.isDebugEnabled())
                    this.debug("rec'd " + msg + " for " + tx + " not expecting a response; ignoring");
                return;
            }

            // Check result
            if (this.log.isTraceEnabled())
                this.trace("rec'd " + msg + " for " + tx);
            if (msg.isSuccess()) {
                tx.setCommitTerm(msg.getCommitTerm());
                tx.setCommitIndex(msg.getCommitIndex());
                if (msg.getCommitLeaderLeaseTimeout() != null)
                    this.commitLeaderLeaseTimeoutMap.put(tx.getTxId(), msg.getCommitLeaderLeaseTimeout());
                tx.setState(TxState.COMMIT_WAITING);
                tx.getMutableView().disableReadTracking();                                  // we no longer need this info
                this.raft.requestService(new CheckWaitingTransactionService(tx));
            } else
                this.raft.fail(tx, new RetryTransactionException(tx, msg.getErrorMessage()));
        }

        @Override
        public void caseInstallSnapshot(InstallSnapshot msg) {

            // Restart election timer (if running)
            if (this.electionTimer.isRunning())
                this.restartElectionTimer();

            // Do we have an existing install?
            boolean startNewInstall = false;
            if (this.snapshotReceive != null) {

                // Does the message not match?
                if (!this.snapshotReceive.matches(msg)) {

                    // If the message is NOT the first one in a new install, ignore it
                    if (msg.getPairIndex() != 0) {
                        if (this.log.isDebugEnabled())
                            this.debug("rec'd " + msg + " which doesn't match in-progress " + this.snapshotReceive + "; ignoring");
                        return;
                    }

                    // The message is the first one in a new install, so discard the existing install
                    this.info("rec'd initial " + msg + " with in-progress " + this.snapshotReceive + "; aborting previous install");
                    startNewInstall = true;
                }
            } else {

                // If the message is NOT the first one in a new install, ignore it
                if (msg.getPairIndex() != 0) {
                    this.info("rec'd non-initial " + msg + " with no in-progress snapshot install; ignoring");
                    return;
                }
            }

            // Get snapshot term and index
            final long term = msg.getSnapshotTerm();
            final long index = msg.getSnapshotIndex();

            // Set up new install if necessary
            if (this.snapshotReceive == null || startNewInstall) {
                assert msg.getPairIndex() == 0;
                if (!this.raft.resetStateMachine(false))
                    return;
                this.updateElectionTimer();
                this.snapshotReceive = new SnapshotReceive(
                  PrefixKVStore.create(this.raft.kv, STATE_MACHINE_PREFIX), term, index, msg.getSnapshotConfig());
                this.info("starting new snapshot install from \"" + msg.getSenderId() + "\" of "
                  + index + "t" + term + " with config " + msg.getSnapshotConfig());
            }
            assert this.snapshotReceive.matches(msg);

            // Apply next chunk of key/value pairs
            if (this.log.isDebugEnabled())
                this.debug("applying " + msg + " to " + this.snapshotReceive);
            try {
                this.snapshotReceive.applyNextChunk(msg.getData());
            } catch (Exception e) {
                this.error("error applying snapshot to key/value store; resetting state machine", e);
                if (!this.raft.resetStateMachine(true))
                    return;
                this.updateElectionTimer();
                this.snapshotReceive = null;
                return;
            }

            // If that was the last chunk, finalize persistent state
            if (msg.isLastChunk()) {
                final Map<String, String> snapshotConfig = this.snapshotReceive.getSnapshotConfig();
                this.info("snapshot install from \"" + msg.getSenderId() + "\" of " + index + "t" + term
                  + " with config " + snapshotConfig + " complete");
                this.snapshotReceive = null;
                if (!this.raft.recordLastApplied(term, index, snapshotConfig))
                    this.raft.resetStateMachine(true);                          // if this fails we are really screwed
                this.updateElectionTimer();
            }
        }

        @Override
        public void caseRequestVote(RequestVote msg) {

            // Did we already vote for somebody else?
            final String peer = msg.getSenderId();
            if (this.votedFor != null && !this.votedFor.equals(peer)) {
                this.info("rec'd " + msg + "; rejected because we already voted for \"" + this.votedFor + "\"");
                return;
            }

            // Verify that we are allowed to vote for this peer
            if (msg.getLastLogTerm() < this.raft.getLastLogTerm()
              || (msg.getLastLogTerm() == this.raft.getLastLogTerm() && msg.getLastLogIndex() < this.raft.getLastLogIndex())) {
                this.info("rec'd " + msg + "; rejected because their log " + msg.getLastLogIndex() + "t"
                  + msg.getLastLogTerm() + " loses to ours " + this.raft.getLastLogIndex() + "t" + this.raft.getLastLogTerm());
                return;
            }

            // Persist our vote for this peer (if not already persisted)
            if (this.votedFor == null) {
                this.info("granting vote to \"" + peer + "\" in term " + this.raft.currentTerm);
                if (!this.updateVotedFor(peer))
                    return;
            } else
                this.info("confirming existing vote for \"" + peer + "\" in term " + this.raft.currentTerm);

            // Send reply
            this.raft.sendMessage(new GrantVote(this.raft.clusterId, this.raft.identity, peer, this.raft.currentTerm));
        }

        @Override
        public void caseGrantVote(GrantVote msg) {

            // Ignore - we already lost the election to the real leader
            if (this.log.isDebugEnabled())
                this.debug("ignoring " + msg + " rec'd while in " + this);
        }

    // Helper methods

        /**
         * Record the peer voted for in the current term.
         */
        private boolean updateVotedFor(String recipient) {

            // Sanity check
            assert Thread.holdsLock(this.raft);
            assert recipient != null;

            // Update persistent store
            final Writes writes = new Writes();
            writes.getPuts().put(VOTED_FOR_KEY, this.raft.encodeString(recipient));
            try {
                this.raft.kv.mutate(writes, true);
            } catch (Exception e) {
                this.error("error persisting vote for \"" + recipient + "\"", e);
                return false;
            }

            // Done
            this.votedFor = recipient;
            return true;
        }

    // Object

        @Override
        public String toString() {
            return this.toStringPrefix()
              + (this.leader != null ? ",leader=\"" + this.leader + "\"" : "")
              + (this.votedFor != null ? ",votedFor=\"" + this.votedFor + "\"" : "")
              + (!this.pendingRequests.isEmpty() ? ",pendingRequests=" + this.pendingRequests.keySet() : "")
              + (!this.pendingWrites.isEmpty() ? ",pendingWrites=" + this.pendingWrites.keySet() : "")
              + (!this.commitLeaderLeaseTimeoutMap.isEmpty() ? ",leaseTimeouts=" + this.commitLeaderLeaseTimeoutMap.keySet() : "")
              + "]";
        }

    // Debug

        @Override
        boolean checkState() {
            if (!super.checkState())
                return false;
            assert this.leaderAddress != null || this.leader == null;
            assert this.electionTimer.isRunning() == this.raft.isClusterMember();
            for (Map.Entry<Long, PendingRequest> entry : this.pendingRequests.entrySet()) {
                final long txId = entry.getKey();
                final PendingRequest pendingRequest = entry.getValue();
                final RaftKVTransaction tx = pendingRequest.getTx();
                assert txId == tx.getTxId();
                assert tx.getState().equals(TxState.COMMIT_READY);
                assert tx.getCommitTerm() == 0;
                assert tx.getCommitIndex() == 0;
            }
            for (Map.Entry<Long, PendingWrite> entry : this.pendingWrites.entrySet()) {
                final long txId = entry.getKey();
                final PendingWrite pendingWrite = entry.getValue();
                final RaftKVTransaction tx = pendingWrite.getTx();
                assert txId == tx.getTxId();
                assert tx.getState().equals(TxState.COMMIT_READY) || tx.getState().equals(TxState.COMMIT_WAITING);
                assert pendingWrite.getFileWriter().getFile().exists();
            }
            return true;
        }

    // PendingRequest

        // Represents a transaction in COMMIT_READY for which a CommitRequest has been sent to the leader
        // but no CommitResponse has yet been received
        private class PendingRequest {

            private final RaftKVTransaction tx;

            PendingRequest(RaftKVTransaction tx) {
                this.tx = tx;
                assert !FollowerRole.this.pendingRequests.containsKey(tx.getTxId());
                FollowerRole.this.pendingRequests.put(tx.getTxId(), this);
            }

            public RaftKVTransaction getTx() {
                return this.tx;
            }
        }

    // PendingWrite

        // Represents a read-write transaction in COMMIT_READY or COMMIT_WAITING for which the server's AppendRequest
        // will have null mutationData, because we will already have the data on hand waiting in a temporary file. This
        // is a simple optimization to avoid sending the same data from leader -> follower just sent from follower -> leader.
        private class PendingWrite {

            private final RaftKVTransaction tx;
            private final FileWriter fileWriter;

            PendingWrite(RaftKVTransaction tx, FileWriter fileWriter) {
                this.tx = tx;
                this.fileWriter = fileWriter;
            }

            public RaftKVTransaction getTx() {
                return this.tx;
            }

            public FileWriter getFileWriter() {
                return this.fileWriter;
            }

            public void cleanup() {
                this.fileWriter.getFile().delete();
                Util.closeIfPossible(this.fileWriter);
            }
        }
    }

// CANDIDATE role

    private static class CandidateRole extends NonLeaderRole {

        private final HashSet<String> votes = new HashSet<>();

    // Constructors

        public CandidateRole(RaftKVDatabase raft) {
            super(raft, true);
        }

    // Lifecycle

        @Override
        public void setup() {
            super.setup();

            // Increment term
            if (!this.raft.advanceTerm(this.raft.currentTerm + 1))
                return;

            // Request votes from other peers
            final HashSet<String> voters = new HashSet<>(this.raft.currentConfig.keySet());
            voters.remove(this.raft.identity);
            this.info("entering candidate role in term " + this.raft.currentTerm + "; requesting votes from " + voters);
            for (String voter : voters) {
                this.raft.sendMessage(new RequestVote(this.raft.clusterId, this.raft.identity, voter,
                  this.raft.currentTerm, this.raft.getLastLogTerm(), this.raft.getLastLogIndex()));
            }
        }

    // Service

        @Override
        public void outputQueueEmpty(String address) {
            // nothing to do
        }

    // Transactions

        @Override
        public void checkReadyTransaction(RaftKVTransaction tx) {

            // Sanity check
            assert Thread.holdsLock(this.raft);
            assert tx.getState().equals(TxState.COMMIT_READY);

            // Check snapshot isolation
            if (this.checkReadyTransactionReadOnlySnapshot(tx))
                return;

            // We can't do anything with it until we have a leader
        }

        @Override
        public void cleanupForTransaction(RaftKVTransaction tx) {
            // nothing to do
        }

    // MessageSwitch

        @Override
        public void caseAppendRequest(AppendRequest msg) {
            this.info("rec'd " + msg + " in " + this + "; reverting to follower");
            this.raft.changeRole(new FollowerRole(this.raft, msg.getSenderId(), this.raft.returnAddress));
            this.raft.receiveMessage(this.raft.returnAddress, msg);
        }

    // MessageSwitch

        @Override
        public void caseCommitResponse(CommitResponse msg) {

            // We could not have ever sent a CommitRequest in this term
            this.failUnexpectedMessage(msg);
        }

        @Override
        public void caseInstallSnapshot(InstallSnapshot msg) {

            // We could not have ever sent an AppendResponse in this term
            this.failUnexpectedMessage(msg);
        }

        @Override
        public void caseRequestVote(RequestVote msg) {

            // Ignore - we are also a candidate and have already voted for ourself
            if (this.log.isDebugEnabled())
                this.debug("ignoring " + msg + " rec'd while in " + this);
        }

        @Override
        public void caseGrantVote(GrantVote msg) {

            // Record vote
            this.votes.add(msg.getSenderId());
            this.info("rec'd election vote from \"" + msg.getSenderId() + "\" in term " + this.raft.currentTerm);

            // Tally votes
            final int allVotes = this.raft.currentConfig.size();
            final int numVotes = this.votes.size() + (this.raft.isClusterMember() ? 1 : 0);
            final int minVotes = allVotes / 2 + 1;                              // require a majority

            // Did we win?
            if (numVotes >= minVotes) {
                this.info("won the election for term " + this.raft.currentTerm + " with "
                  + numVotes + "/" + allVotes + " votes; BECOMING LEADER IN TERM " + this.raft.currentTerm);
                this.raft.changeRole(new LeaderRole(this.raft));
            }
        }

    // Object

        @Override
        public String toString() {
            return this.toStringPrefix()
              + ",votes=" + this.votes
              + "]";
        }

    // Debug

        @Override
        boolean checkState() {
            if (!super.checkState())
                return false;
            assert this.electionTimer.isRunning();
            assert this.raft.isClusterMember();
            return true;
        }
    }

// Prefix Functions

    private abstract static class AbstractPrefixFunction<F, T> implements Function<F, T> {

        protected final byte[] prefix;

        AbstractPrefixFunction(byte[] prefix) {
            this.prefix = prefix;
        }
    }

    private static class PrefixKeyRangeFunction extends AbstractPrefixFunction<KeyRange, KeyRange> {

        PrefixKeyRangeFunction(byte[] prefix) {
            super(prefix);
        }

        @Override
        public KeyRange apply(KeyRange range) {
            return range.prefixedBy(this.prefix);
        }
    }

    private static class PrefixPutFunction extends AbstractPrefixFunction<Map.Entry<byte[], byte[]>, Map.Entry<byte[], byte[]>> {

        PrefixPutFunction(byte[] prefix) {
            super(prefix);
        }

        @Override
        public Map.Entry<byte[], byte[]> apply(Map.Entry<byte[], byte[]> entry) {
            return new AbstractMap.SimpleEntry<byte[], byte[]>(Bytes.concat(this.prefix, entry.getKey()), entry.getValue());
        }
    }

    private static class PrefixAdjustFunction extends AbstractPrefixFunction<Map.Entry<byte[], Long>, Map.Entry<byte[], Long>> {

        PrefixAdjustFunction(byte[] prefix) {
            super(prefix);
        }

        @Override
        public Map.Entry<byte[], Long> apply(Map.Entry<byte[], Long> entry) {
            return new AbstractMap.SimpleEntry<byte[], Long>(Bytes.concat(this.prefix, entry.getKey()), entry.getValue());
        }
    }

// Debug/Sanity Checking

    private abstract class ErrorLoggingRunnable implements Runnable {

        @Override
        public final void run() {
            try {
                this.doRun();
            } catch (Throwable t) {
                RaftKVDatabase.this.error("exception in callback", t);
            }
        }

        protected abstract void doRun();
    }

    private boolean checkState() {
        try {
            this.doCheckState();
        } catch (AssertionError e) {
            throw new AssertionError("checkState() failure for \"" + this.identity + "\"", e);
        }
        return true;
    }

    private void doCheckState() {
        assert Thread.holdsLock(this);

        // Handle stopped state
        if (this.role == null) {
            assert this.kv == null;
            assert this.random == null;
            assert this.currentTerm == 0;
            assert this.commitIndex == 0;
            assert this.lastAppliedTerm == 0;
            assert this.lastAppliedIndex == 0;
            assert this.lastAppliedConfig == null;
            assert this.currentConfig == null;
            assert this.clusterId == 0;
            assert this.raftLog.isEmpty();
            assert this.logDirChannel == null;
            assert this.executor == null;
            assert this.transmitting.isEmpty();
            assert this.openTransactions.isEmpty();
            assert this.pendingService.isEmpty();
            assert !this.shuttingDown;
            return;
        }

        // Handle running state
        assert this.kv != null;
        assert this.random != null;
        assert this.executor != null;
        assert this.logDirChannel != null;
        assert !this.executor.isShutdown() || this.shuttingDown;

        assert this.currentTerm >= 0;
        assert this.commitIndex >= 0;
        assert this.lastAppliedTerm >= 0;
        assert this.lastAppliedIndex >= 0;
        assert this.lastAppliedConfig != null;
        assert this.currentConfig != null;

        assert this.currentTerm >= this.lastAppliedTerm;
        assert this.commitIndex >= this.lastAppliedIndex;
        assert this.commitIndex <= this.lastAppliedIndex + this.raftLog.size();
        long index = this.lastAppliedIndex;
        long term = this.lastAppliedTerm;
        for (LogEntry logEntry : this.raftLog) {
            assert logEntry.getIndex() == index + 1;
            assert logEntry.getTerm() >= term;
            index = logEntry.getIndex();
            term = logEntry.getTerm();
        }

        // Check configured vs. unconfigured
        if (this.isConfigured()) {
            assert this.clusterId != 0;
            assert this.currentTerm > 0;
            assert this.lastAppliedTerm >= 0;
            assert this.lastAppliedIndex >= 0;
            assert !this.currentConfig.isEmpty();
            assert this.currentConfig.equals(this.buildCurrentConfig());
            assert this.getLastLogTerm() > 0;
            assert this.getLastLogIndex() > 0;
        } else {
            assert this.lastAppliedTerm == 0;
            assert this.lastAppliedIndex == 0;
            assert this.lastAppliedConfig.isEmpty();
            assert this.currentConfig.isEmpty();
            assert this.raftLog.isEmpty();
        }

        // Check role
        assert this.role.checkState();

        // Check transactions
        for (RaftKVTransaction tx : this.openTransactions.values())
            tx.checkStateOpen(this.currentTerm, this.getLastLogIndex(), this.commitIndex);
    }
}

