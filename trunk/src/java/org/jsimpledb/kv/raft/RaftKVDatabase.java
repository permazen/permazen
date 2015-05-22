
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
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
import java.util.Set;
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
import org.jsimpledb.kv.KVTransaction;
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
 * Raft defines a distributed algorithm for maintaining a shared state machine. {@link RaftKVDatabase} turns
 * this into a transactional key/value database with linearizable ACID semantics:
 *  <ul>
 *  <li>The Raft state machine is the key/value store data.</li>
 *  <li>Unapplied log entries are stored on disk as serialized {@link Writes}, and also cached in memory.</li>
 *  <li>On leaders only, the {@link Writes} associated with some number of the most recently applied log entries
 *      are cached in memory for time <i>T<sub>max</sub></i> since creation, where <i>T<sub>max</sub></i> is the
 *      maximum supported transaction duration (see below for how these cached {@link Writes} are used).
 *      This caching is subject to <i>M<sub>max</sub></i>, which limits the total memory used by memory-cached
 *      {@link Writes}; if reached, these cached {@link Writes} are discarded early.</li>
 *  <li>Concurrent transactions are supported through a simple optimistic locking MVCC scheme (same as used by
 *      {@link org.jsimpledb.kv.mvcc.SnapshotKVDatabase}):
 *      <ul>
 *      <li>Transactions execute locally until commit time, using a {@link MutableView} to collect mutations.
 *          The {@link MutableView} is based on the local node's most recent log entry (whether committed or not);
 *          this is called the <i>base term and index</i> for the transaction.</li>
 *      <li>On commit, the transaction's {@link Reads}, {@link Writes}, and base index and term are
 *          {@linkplain CommitRequest sent} to the leader.</li>
 *      <li>The leader confirms that the log entry corresponding to the transaction's base index and term either:
 *          <ul>
 *          <li>Is still present in its own log and not yet applied to the state machine (in which case the
 *              {@link Writes} are available with the log entry); or</li>
 *          <li>Corresponds to an already applied log entry whose {@link Writes} are still cached in memory.</li>
 *          </ul>
 *          If this is not the case, then the transaction's base log entry is too old (older than <i>T<sub>max</sub></i>,
 *          or was committed early due to memory pressure), and so the transaction is rejected with a
 *          {@link RetryTransactionException}. Otherwise, the {@link Writes} associated with all log entries
 *          after the transaction's base log entry are available.
 *      <li>The leader confirms that the {@link Writes} associated with log entries created after the transaction's
 *          base log entry do not create linearizability violations when {@linkplain Reads#isConflict compared against}
 *          the transaction's {@link Reads}. If so, the transaction is rejected with a retry.</li>
 *      <li>The leader adds a new log entry consisting of the transaction's {@link Writes} to its log.
 *          The associated term and index become the transaction's <i>commit term and index</i>; the leader
 *          then {@linkplain CommitResponse replies} to the follower with this information.</li>
 *      <li>When the follower sees a log entry appear in its log matching the transaction's commit term and index, and
 *          that log entry has been committed (in the Raft sense), the transaction is also committed.</li>
 *      <li>As an optimization, when the leader sends a log entry to the same follower who committed the corresponding
 *          transaction in the first place, only the transaction ID is sent, because the follower already has the data.</li>
 *      </ul>
 *  </li>
 *  <li>For transactions occurring on a leader, the logic is the same except of course no network communication occurs.</li>
 *  <li>For read-only transactions, the leader does not create a new log entry; instead, the transaction's commit
 *      term and index are set to the term and index of the last entry in the leader's log. The leader also calculates its
 *      current "leader lease timeout", which is the earliest time at which it is possible for another leader to be elected.
 *      This is calculated as the time in the past at which the leader sent {@link AppendRequest}'s to a majority of followers
 *      who have since responded, plus the {@linkplain #setMinElectionTimeout minimum election timeout}, minus a small adjustment
 *      for possible clock drift (this assumes all nodes have the same minimum election timeout configured). If the current
 *      time is prior to the leader lease timeout, the transaction may be committed immediately; otherwise, the current time
 *      is returned to the follower as minimum required leader lease timeout before the transaction may be committed.</li>
 *  <li>Included with each {@link AppendRequest} is the leader's current timestamp and lease timeout, so followers can commit
 *      any waiting read-only transactions. Leaders keep track of which followers are waiting on which leader lease
 *      timeout values, and when the leader lease timeout advances to allow a follower to commit a transaction, the follower
 *      is notified with an immediate {@link AppendRequest} update.</li>
 *  <li>A weaker consistency guarantee for read-only transactions (serializable but not linearizable) is also possible; see
 *      {@link RaftKVTransaction#setReadOnlySnapshot RaftKVTransaction.setReadOnlySnapshot()}.</li>
 *  </ul>
 *
 * <p>
 * An {@link AtomicKVStore} is required to store local persistent state; if none is configured, a {@link LevelDBKVStore} is used.
 * </p>
 *
 * @see <a href="https://raftconsensus.github.io/">The Raft Consensus Algorithm</a>
 */
public class RaftKVDatabase implements KVDatabase {

    /**
     * Default minimum election timeout ({@value #DEFAULT_MIN_ELECTION_TIMEOUT}ms).
     *
     * @see #setMinElectionTimeout
     */
    public static final int DEFAULT_MIN_ELECTION_TIMEOUT = 250;

    /**
     * Default maximum election timeout ({@value #DEFAULT_MAX_ELECTION_TIMEOUT}ms).
     *
     * @see #setMaxElectionTimeout
     */
    public static final int DEFAULT_MAX_ELECTION_TIMEOUT = 400;

    /**
     * Default heartbeat timeout ({@value DEFAULT_HEARTBEAT_TIMEOUT}ms).
     *
     * @see #setHeartbeatTimeout
     */
    public static final int DEFAULT_HEARTBEAT_TIMEOUT = 60;

    /**
     * Default maximum supported outstanding transaction duration ({@value DEFAULT_MAX_TRANSACTION_DURATION}ms).
     *
     * @see #setMaxTransactionDuration
     */
    public static final int DEFAULT_MAX_TRANSACTION_DURATION = 10 * 1000;

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
    private static final int MAX_APPLIED_LOG_ENTRIES = 32;
    private static final float MAX_CLOCK_DRIFT = 0.01f;     // max clock drift (ratio) in one min election timeout interval

    // File prefixes and suffixes
    private static final String TX_FILE_PREFIX = "tx-";
    private static final String TEMP_FILE_PREFIX = "temp-";
    private static final String TEMP_FILE_SUFFIX = ".tmp";
    private static final String KVSTORE_FILE_SUFFIX = ".kvstore";
    private static final Pattern TEMP_FILE_PATTERN = Pattern.compile(".*" + Pattern.quote(TEMP_FILE_SUFFIX));
    private static final Pattern KVSTORE_FILE_PATTERN = Pattern.compile(".*" + Pattern.quote(KVSTORE_FILE_SUFFIX));

    // Keys for persistent Raft state
    private static final byte[] CURRENT_TERM_KEY = ByteUtil.parse("0001");
    private static final byte[] VOTED_FOR_KEY = ByteUtil.parse("0002");
    private static final byte[] LAST_APPLIED_TERM_KEY = ByteUtil.parse("0003");
    private static final byte[] LAST_APPLIED_INDEX_KEY = ByteUtil.parse("0004");

    // Prefix for all state machine key/value keys
    private static final byte[] STATE_MACHINE_PREFIX = ByteUtil.parse("80");

    // Logging
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    // Configuration state
    private Network network = new TCPNetwork();
    private String identity;
    private AtomicKVStore kvstore;
    private final HashBiMap<String, String> peerMap = HashBiMap.<String, String>create();       // maps identity -> network address
    private int minElectionTimeout = DEFAULT_MIN_ELECTION_TIMEOUT;
    private int maxElectionTimeout = DEFAULT_MAX_ELECTION_TIMEOUT;
    private int heartbeatTimeout = DEFAULT_HEARTBEAT_TIMEOUT;
    private int maxTransactionDuration = DEFAULT_MAX_TRANSACTION_DURATION;
    private int commitTimeout = DEFAULT_COMMIT_TIMEOUT;
    private long maxAppliedLogMemory = DEFAULT_MAX_APPLIED_LOG_MEMORY;
    private File logDir;

    // Raft runtime state
    private Role role;                                                  // Raft state: LEADER, FOLLOWER, or CANDIDATE
    private SecureRandom random;                                        // used to randomize election timeout
    private long currentTerm;
    private long commitIndex;
    private long lastAppliedTerm;                                       // key/value store term
    private long lastAppliedIndex;                                      // key/value store index
    private final ArrayList<LogEntry> raftLog = new ArrayList<>();      // log entries not yet applied to key/value store

    // Non-Raft runtime state
    private AtomicKVStore kv;
    private FileChannel logDirChannel;
    private ScheduledExecutorService executor;
    private final LinkedHashSet<Service> pendingService = new LinkedHashSet<>();
    private final HashSet<String> writablePeers = new HashSet<>();
    private final HashMap<Long, RaftKVTransaction> openTransactions = new HashMap<>();
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
     * @param identity unique Raft identity
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setIdentity(String identity) {
        Preconditions.checkState(this.role == null, "already started");
        this.identity = identity;
    }

    /**
     * Configure the Raft peers.
     *
     * <p>
     * Required property unless this is a single-node cluster.
     *
     * @param peers mapping from peer identity to unique {@link Network} address, or null for none
     * @throws IllegalStateException if this instance is already started
     * @throws IllegalArgumentException if any key or value in {@code peers} is null
     * @throws IllegalArgumentException if any peer {@link Network} address is not unique
     */
    public synchronized void setPeers(Map<String, String> peers) {
        Preconditions.checkArgument(peers != null, "null peers");
        Preconditions.checkState(this.role == null, "already started");
        this.peerMap.clear();
        if (peers != null) {
            for (Map.Entry<String, String> entry : peers.entrySet()) {
                final String peer = entry.getKey();
                final String address = entry.getValue();
                Preconditions.checkArgument(peer != null && peer.length() > 0, "invalid null/empty peer identity");
                Preconditions.checkArgument(address != null, "invalid null network address");
                try {
                    this.peerMap.put(peer, address);
                } catch (IllegalArgumentException e) {
                    this.peerMap.clear();
                    throw new IllegalArgumentException("non-unique network address " + address
                      + " shared by peers " + peer + " and " + this.peerMap.inverse().get(address));
                }
            }
        }
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
        this.peerMap.remove(this.identity);

        // Log
        this.info("starting " + this + " in directory " + this.logDir + " with peers " + this.peerMap);

        // Start up local database
        boolean success = false;
        try {

            // Create/verify log directory
            if (this.logDir.exists()) {
                if (!this.logDir.isDirectory()) {
                    throw new IOException("cannot create directory `" + this.logDir
                      + "' because a non-directory file with the same name already exists");
                }
            } else if (!this.logDir.mkdirs())
                throw new IOException("failed to create directory `" + this.logDir + "'");

            // By default use an atomic key/value store based on LevelDB if not explicitly specified
            if (this.kvstore != null)
                this.kv = this.kvstore;
            else {
                final File leveldbDir = new File(this.logDir, "levedb" + KVSTORE_FILE_SUFFIX);
                if (!leveldbDir.mkdirs())
                    throw new IOException("failed to create directory `" + leveldbDir + "'");
                this.kv = new LevelDBKVStore(new Iq80DBFactory().open(leveldbDir, new Options().createIfMissing(true)));
            }

            // Open directory so we have a way to fsync() it
            assert this.logDirChannel == null;
            this.logDirChannel = FileChannel.open(this.logDir.toPath(), StandardOpenOption.READ);

            // Create randomizer
            assert this.random == null;
            this.random = new SecureRandom();

            // Mark all peers as initially writable
            assert this.writablePeers.isEmpty();
            this.writablePeers.addAll(this.peerMap.keySet());

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
            this.currentTerm = this.decodeLong(CURRENT_TERM_KEY, 0);
            final String votedFor = this.decodeString(VOTED_FOR_KEY, null);
            this.lastAppliedTerm = this.decodeLong(LAST_APPLIED_TERM_KEY, 0);
            this.lastAppliedIndex = this.decodeLong(LAST_APPLIED_INDEX_KEY, 0);
            this.info("recovered Raft state:\n  currentTerm=" + this.currentTerm
              + "\n  votedFor=" + (votedFor != null ? "\"" + votedFor + "\"" : "nobody")
              + "\n  lastApplied=" + this.lastAppliedTerm + "/" + this.lastAppliedIndex);

            // If we crashed part way through a snapshot install, recover
            if (this.lastAppliedTerm < 0 || this.lastAppliedIndex < 0) {
                this.info("detected partially applied snapshot, resetting state machine");
                if (!this.resetStateMachine(true))
                    throw new IOException("error resetting state machine");
            }

            // Initialize commit index
            this.commitIndex = this.lastAppliedIndex;

            // Reload outstanding log entries from disk
            this.loadLog();

            // Start as follower (with unknown leader)
            this.changeRole(new FollowerRole(this, null, votedFor));

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
        this.lastAppliedTerm = 0;
        this.lastAppliedIndex = 0;
        this.writablePeers.clear();
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
                file.delete();
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
                logEntry.getFile().delete();
                i.remove();
            } else {
                expectedIndex++;
                lastTermSeen = logEntry.getTerm();
            }
        }
        this.info("recovered " + this.raftLog.size() + " Raft log entries: " + this.raftLog);
    }

// Transactions

    /**
     * Create a new transaction.
     *
     * @throws IllegalStateException if this instance is not {@linkplain #start started} or in the process of shutting down
     */
    @Override
    public synchronized KVTransaction createTransaction() {

        // Sanity check
        assert this.checkState();
        Preconditions.checkState(this.role != null, "not started");
        Preconditions.checkState(!this.shuttingDown, "shutting down");

        // Grab a snapshot of our local database
        final CloseableKVStore snapshot = this.kv.snapshot();

        // Create a view of just the state machine keys and values
        KVStore kview = PrefixKVStore.create(snapshot, STATE_MACHINE_PREFIX);

        // Base transaction on the most recent log entry, if any, otherwise directly on the committed key/value store.
        // This is itself a form of optimistic locking: we assume that the most recent log entry has a high probability of
        // being committed (in the Raft sense), which is of course required in order to commit any new transaction based on it.
        long txTerm = this.lastAppliedTerm;
        long txIndex = this.lastAppliedIndex;
        for (LogEntry logEntry : this.raftLog) {
            final Writes writes = logEntry.getWrites();
            if (!writes.isEmpty())
                kview = new MutableView(kview, null, logEntry.getWrites());             // stack up MutableViews
            txTerm = logEntry.getTerm();
            txIndex = logEntry.getIndex();
        }

        // Create transaction
        final RaftKVTransaction tx = new RaftKVTransaction(this, txTerm, txIndex, snapshot, new MutableView(kview));
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

    abstract static class Service implements Runnable {

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
        public boolean needsService() {

            // Sanity check
            assert Thread.holdsLock(RaftKVDatabase.this);

            // Has timer expired?
            final int overdueMillis = -this.timeoutDeadline.offsetFromNow();
            if (this.pendingTimeout == null || overdueMillis < 0)
                return false;

            // Yes, timer requires service
            if (Timer.this.log.isTraceEnabled())
                RaftKVDatabase.this.trace(Timer.this.name + " expired " + overdueMillis + "ms ago");
            this.cancel();
            return true;
        }

        /**
         * Determine if this timer is running, i.e., will expire or has expired but {@link #needsService} has not been invoked yet.
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
        this.info("resetting state machine");

        // Set invalid values while we make non-atomic changes, in case we crash in the middle
        if (!this.updateStateMachine(-1, -1))
            return false;

        // Delete all key/value pairs
        this.kv.removeRange(STATE_MACHINE_PREFIX, ByteUtil.getKeyAfterPrefix(STATE_MACHINE_PREFIX));
        assert !this.kv.getRange(STATE_MACHINE_PREFIX, ByteUtil.getKeyAfterPrefix(STATE_MACHINE_PREFIX), false).hasNext();

        // Delete all log files
        this.raftLog.clear();
        for (File file : this.logDir.listFiles()) {
            if (LogEntry.LOG_FILE_PATTERN.matcher(file.getName()).matches())
                file.delete();
        }

        // Optionally finish intialization
        if (initialize && !this.updateStateMachine(0, 0))
            return false;

        // Done
        this.info("done resetting state machine");
        return true;
    }

    /**
     * Update the persisted state machine term and index.
     */
    private boolean updateStateMachine(long term, long index) {

        // Sanity check
        assert Thread.holdsLock(this);
        if (this.log.isTraceEnabled())
            this.trace("updating state machine last applied term/index to " + term + "/" + index);

        // Prepare updates
        final Writes writes = new Writes();
        writes.getPuts().put(LAST_APPLIED_TERM_KEY, LongEncoder.encode(term));
        writes.getPuts().put(LAST_APPLIED_INDEX_KEY, LongEncoder.encode(index));

        // Update persistent store
        try {
            this.kv.mutate(writes, true);
        } catch (Exception e) {
            this.error("error updating key/value store term/index to " + term + "/" + index, e);
            return false;
        }

        // Update in-memory copy
        this.lastAppliedTerm = Math.max(term, 0);
        this.lastAppliedIndex = Math.max(index, 0);
        this.commitIndex = this.lastAppliedIndex;
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
     * @param writes log entry mutations
     * @return new {@link LogEntry}
     * @throws Exception if an error occurs
     * @throws IllegalArgumentException if {@code writes} is null
     */
    private LogEntry appendLogEntry(long term, Writes writes) throws Exception {

        // Sanity check
        Preconditions.checkArgument(writes != null, "null writes");

        // Serialize writes to temporary file
        final File tempFile = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX, this.logDir);
        try (FileWriter output = new FileWriter(tempFile)) {
            writes.serialize(output);
        }

        // Append log entry
        return this.appendLogEntry(term, writes, tempFile);
    }

    /**
     * Append a log entry to the Raft log.
     *
     * @param term new log entry term
     * @param writesData encoded writes data
     * @return new {@link LogEntry}
     * @throws Exception if an error occurs
     * @throws IllegalArgumentException if {@code writesData} is null
     */
    private LogEntry appendLogEntry(long term, ByteBuffer writesData) throws Exception {

        // Sanity check
        Preconditions.checkArgument(writesData != null, "null writesData");

        // Deserialize data
        final Writes writes = Writes.deserialize(new ByteBufferInputStream(writesData.asReadOnlyBuffer()));

        // Write data to temporary file
        final File tempFile = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX, this.logDir);
        try (FileWriter output = new FileWriter(tempFile)) {
            while (writesData.hasRemaining())
                output.getFileOutputStream().getChannel().write(writesData);
        }

        // Append log entry
        return this.appendLogEntry(term, writes, tempFile);
    }

    /**
     * Append a log entry to the Raft log.
     *
     * @param term new log entry term
     * @param writes log entry mutations
     * @param file file containing serialized {@link writes}; content must be already durably persisted; will be renamed
     * @return new {@link LogEntry}
     * @throws Exception if an error occurs
     * @throws IllegalArgumentException if {@code writesData} is null
     */
    private LogEntry appendLogEntry(long term, Writes writes, File file) throws Exception {

        // Sanity check
        assert Thread.holdsLock(this);
        assert this.role != null;
        Preconditions.checkArgument(term > 0, "term <= 0");
        Preconditions.checkArgument(writes != null, "null writes");
        Preconditions.checkArgument(file != null, "null file");

        // Get file length
        final long fileLength = Util.getLength(file);

        // Create new log entry
        final LogEntry logEntry = new LogEntry(term, this.getLastLogIndex() + 1, this.logDir, writes, fileLength);
        if (this.log.isDebugEnabled())
            this.debug("adding new log entry " + logEntry + " using " + file.getName());

        // Atomically rename file and fsync() directory to durably persist
        Files.move(file.toPath(), logEntry.getFile().toPath(), StandardCopyOption.ATOMIC_MOVE);
        this.logDirChannel.force(true);

        // Add new log entry to in-memory log
        this.raftLog.add(logEntry);

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

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[identity=" + (this.identity != null ? "\"" + this.identity + "\"" : null)
          + ",logDir=" + this.logDir
          + ",term=" + this.currentTerm
          + ",commitIndex=" + this.commitIndex
          + ",lastApplied=" + this.lastAppliedTerm + "/" + this.lastAppliedIndex
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
        this.receiveMessage(msg);
    }

    private synchronized void outputQueueEmpty(String address) {

        // Sanity check
        assert this.checkState();
        if (this.role == null)
            return;

        // Get peer's identity
        final String peer = this.peerMap.inverse().get(address);
        if (peer == null) {
            this.warn("rec'd output queue empty notification for unknown peer " + address);
            return;
        }

        // Update peer's writable state and request service in case we have something to send
        if (this.writablePeers.add(peer)) {
            if (this.log.isTraceEnabled())
                this.trace("QUEUE_EMPTY notification for peer \"" + peer + "\"");
            this.role.outputQueueEmpty(peer);
        }
    }

// Messages

    private boolean sendMessage(Message msg) {

        // Sanity check
        assert Thread.holdsLock(this);

        // Get peer's address
        final String peer = msg.getRecipientId();
        final String address = this.peerMap.get(peer);
        assert address != null;

        // Send message
        if (this.log.isTraceEnabled())
            this.trace("XMIT " + msg + " to \"" + peer + "\"");
        if (this.network.send(address, msg.encode())) {
            this.writablePeers.remove(peer);
            return true;
        }
        this.warn("transmit of " + msg + " to \"" + peer + "\" failed locally");
        return false;
    }

    private synchronized void receiveMessage(Message msg) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert this.checkState();
        if (this.role == null) {
            if (this.log.isDebugEnabled())
                this.debug("rec'd " + msg + " rec'd in unconfigured state; ignoring");
            return;
        }

        // Sanity check source
        final String peer = msg.getSenderId();
        if (!this.peerMap.containsKey(peer)) {
            this.warn("rec'd " + msg + " from unknown \"" + peer + "\"; ignoring");
            return;
        }

        // Sanity check destination
        final String dest = msg.getRecipientId();
        if (!dest.equals(this.identity)) {
            this.warn("rec'd misdirected " + msg + " intended for \"" + dest + "\"; ignoring");
            return;
        }

        // Is sender's term too low? Ignore it
        if (msg.getTerm() < this.currentTerm) {
            this.info("rec'd " + msg + " with term " + msg.getTerm() + " < " + this.currentTerm + " from "
              + peer + ", ignoring");
            return;
        }

        // Is my term too low? If so update and revert to follower
        if (msg.getTerm() > this.currentTerm) {

            // First check with current role; in some special cases we ignore this
            if (!this.role.mayAdvanceCurrentTerm(msg)) {
                if (this.log.isTraceEnabled()) {
                    this.trace("rec'd " + msg + " with term " + msg.getTerm() + " > " + this.currentTerm + " from "
                      + peer + " but current role say to ignore it");
                }
                return;
            }

            // Revert to follower
            this.info("rec'd " + msg.getClass().getSimpleName() + " with term " + msg.getTerm() + " > " + this.currentTerm
              + " from \"" + peer + "\", updating term and "
              + (this.role instanceof FollowerRole ? "remaining a" : "reverting to") + " follower");
            if (!this.advanceTerm(msg.getTerm()))
                return;
            this.changeRole(new FollowerRole(this, msg.isLeaderMessage() ? peer : null));
        }

        // Debug
        if (this.log.isTraceEnabled())
            this.trace("RECV " + msg + " in " + this.role);

        // Handle message
        msg.visit(this.role);
    }

// Utility methods

    private long decodeLong(byte[] key, long defaultValue) {
        final byte[] value = this.kv.get(key);
        if (value == null)
            return defaultValue;
        try {
            return LongEncoder.decode(value);
        } catch (IllegalArgumentException e) {
            this.error("can't interpret encoded long value "
              + ByteUtil.toString(value) + " under key " + ByteUtil.toString(key), e);
            return defaultValue;
        }
    }

    private String decodeString(byte[] key, String defaultValue) {
        final byte[] value = this.kv.get(key);
        if (value == null)
            return defaultValue;
        final DataInputStream input = new DataInputStream(new ByteArrayInputStream(value));
        try {
            return input.readUTF();
        } catch (IOException e) {
            this.error("can't interpret encoded string value "
              + ByteUtil.toString(value) + " under key " + ByteUtil.toString(key), e);
            return defaultValue;
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

    abstract static class Role implements MessageSwitch {

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

        public abstract void outputQueueEmpty(String peer);

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

                // Prepare combined Mutations containing prefixed log entry changes plus my own
                final Writes logWrites = logEntry.getWrites();
                final Writes myWrites = new Writes();
                myWrites.getPuts().put(LAST_APPLIED_TERM_KEY, LongEncoder.encode(logEntry.getTerm()));
                myWrites.getPuts().put(LAST_APPLIED_INDEX_KEY, LongEncoder.encode(logEntry.getIndex()));
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
                this.raft.lastAppliedIndex = logEntry.getIndex();
                this.raft.raftLog.remove(0);

                // Delete the log file
                logEntry.getFile().delete();

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
         * @param tx the transaction
         * @throws KVTransactionException if an error occurs
         */
        public abstract void checkReadyTransaction(RaftKVTransaction tx);

        /**
         * Check a transaction waiting for its log entry to be committed (in the {@link TxState#COMMIT_WAITING} state).
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

            // Has the transaction's log entry been received yet?
            if (commitIndex > this.raft.getLastLogIndex())
                return;

            // Has the transaction's log entry been committed yet?
            if (commitIndex > this.raft.commitIndex)
                return;

            // Verify the term of the committed log entry; if not what we expect, the log entry was overwritten by a new leader
            final long commitTerm = tx.getCommitTerm();
            if (this.raft.getLogTermAtIndex(commitIndex) != commitTerm)
                throw new RetryTransactionException(tx, "leader was deposed during commit");

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
              + ",applied=" + this.raft.lastAppliedTerm + "/" + this.raft.lastAppliedIndex
              + ",commit=" + this.raft.commitIndex
              + ",log=" + this.raft.raftLog
              + "]";
        }
    }

// LEADER role

    private static class LeaderRole extends Role {

        private final ArrayList<LogEntry> appliedLogEntries = new ArrayList<>(); // log entries already applied to key/value store
        private final HashMap<String, Follower> followerMap = new HashMap<>();
        private final Service updateLeaderCommitIndexService = new Service(this, "update leader commitIndex") {
            @Override
            public void run() {
                LeaderRole.this.updateLeaderCommitIndex();
            }
        };
        private final Service pruneAppliedLogEntriesService = new Service(this, "prune applied logs") {
            @Override
            public void run() {
                LeaderRole.this.pruneAppliedLogEntries();
            }
        };
        private final Service updateLeaseTimeoutService = new Service(this, "update lease timeout") {
            @Override
            public void run() {
                LeaderRole.this.updateLeaseTimeout();
            }
        };
        private Timestamp leaseTimeout = new Timestamp();                   // tracks the earliest time we can be deposed

    // Constructors

        public LeaderRole(RaftKVDatabase raft) {
            super(raft);
        }

    // Lifecycle

        @Override
        public void setup() {
            super.setup();
            this.info("entering leader role in term " + this.raft.currentTerm);

            // Create Follower objects to maintain follower state; schedule immediate update probes of all followers
            final long lastLogIndex = this.raft.getLastLogIndex();
            for (String peer : this.raft.peerMap.keySet()) {
                final Follower follower = new Follower(peer, this.raft.getLastLogIndex());
                follower.setUpdateTimer(
                  this.raft.new Timer("update timer for \"" + peer + "\"", new FollowerUpdateService(follower)));
                this.followerMap.put(peer, follower);
                follower.getUpdateTimer().timeoutNow();
            }
        }

        @Override
        public void shutdown() {
            super.shutdown();

            // Clean up followers
            for (Follower follower : this.followerMap.values()) {
                follower.cancelSnapshotTransmit();
                follower.getUpdateTimer().cancel();
            }
        }

    // Service

        @Override
        public void outputQueueEmpty(String peer) {

            // Find follower
            final Follower follower = this.followerMap.get(peer);
            if (follower == null) {
                this.warn("outputQueueEmpty() for unknown peer \"" + peer + "\"; ignoring");
                return;
            }

            // Check for update
            this.checkUpdateFollower(follower);
        }

        @Override
        protected void logEntryApplied(LogEntry logEntry) {

            // Save log entry in the applied log entry list
            this.appliedLogEntries.add(logEntry);

            // Prune applied log entries
            this.raft.requestService(this.pruneAppliedLogEntriesService);
        }

        @Override
        protected boolean mayApplyLogEntry(LogEntry logEntry) {

            // Are we running out of memory? If so, go ahead.
            final long memoryUsed = this.pruneAppliedLogEntries();
            if (memoryUsed > this.raft.maxAppliedLogMemory) {
                if (this.log.isTraceEnabled()) {
                    this.trace("allowing log entry " + logEntry + " to be applied because memory usage is high: "
                      + memoryUsed + " > " + this.raft.maxAppliedLogMemory);
                }
                return true;
            }

            // If any snapshots are in progress, we don't want to apply any log entries with index greater than the snapshot's
            // index, because then we'd "lose" the next log entry to update that follower, and just have to send a snapshot again.
            // However, we impose a limit on how long we'll wait for a slow follower to receive its snapshot.
            for (Follower follower : this.followerMap.values()) {
                final SnapshotTransmit snapshotTransmit = follower.getSnapshotTransmit();
                if (snapshotTransmit != null
                  && snapshotTransmit.getSnapshotIndex() < logEntry.getIndex()              // currently, this will always be true
                  && snapshotTransmit.getAge() < MAX_SNAPSHOT_TRANSMIT_AGE) {
                    if (this.log.isTraceEnabled()) {
                        this.trace("delaying application of " + logEntry + " because of in-progress snapshot install of "
                          + snapshotTransmit.getSnapshotTerm() + "/" + snapshotTransmit.getSnapshotIndex()
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

        private long pruneAppliedLogEntries() {

            // Calculate total memory usage
            long totalLogEntryMemoryUsed = 0;
            for (LogEntry logEntry : Iterables.concat(this.appliedLogEntries, this.raft.raftLog))
                totalLogEntryMemoryUsed += logEntry.getWritesSize();

            // Delete applied log entries to stay under limits
            long numAppliedLogEntries = this.appliedLogEntries.size();
            for (Iterator<LogEntry> i = this.appliedLogEntries.iterator(); i.hasNext(); ) {
                final LogEntry logEntry = i.next();
                if (logEntry.getAge() >= this.raft.maxTransactionDuration
                  || totalLogEntryMemoryUsed > this.raft.maxAppliedLogMemory
                  || numAppliedLogEntries > MAX_APPLIED_LOG_ENTRIES) {
                    i.remove();
                    totalLogEntryMemoryUsed -= logEntry.getWritesSize();
                    numAppliedLogEntries--;
                }
            }

            // Done
            return totalLogEntryMemoryUsed;
        }

        /**
         * Update my {@code commitIndex} based on followers' {@code matchIndex}'s.
         */
        private void updateLeaderCommitIndex() {

            // Update my commit index based on when a majority of cluster members have ack'd log entries
            final long lastLogIndex = this.raft.getLastLogIndex();
            while (this.raft.commitIndex < lastLogIndex) {

                // Get the index in question
                final long index = this.raft.commitIndex + 1;

                // Do a majority of cluster nodes have a copy of this log entry?
                int count = 1;                                                      // count myself
                for (Follower follower : this.followerMap.values()) {
                    if (follower.getMatchIndex() >= index)
                        count++;
                }
                if (count <= (1 + this.followerMap.size()) / 2)                     // note: followerMap does not include myself
                    break;

                // Log entry term must match my current term; however, as a special case, if the log entry
                // is stored on every server, it can be safely considered committed (dissertation, sect. 3.6.2)
                final LogEntry logEntry = this.raft.getLogEntryAtIndex(index);
                if (logEntry.getTerm() != this.raft.currentTerm && count < this.followerMap.size() + 1)
                    break;

                // Update commit index
                if (this.log.isDebugEnabled()) {
                    this.debug("advancing commit index from " + this.raft.commitIndex + " -> " + index
                      + " based on " + count + "/" + (this.followerMap.size() + 1) + " nodes having received " + logEntry);
                }
                this.raft.commitIndex = index;
                this.raft.requestService(this.checkWaitingTransactionsService);
                this.raft.requestService(this.applyCommittedLogEntriesService);
            }
        }

        /**
         * Update my {@code leaseTimeout} based on followers' returned {@code leaderTimeout}'s.
         */
        private void updateLeaseTimeout() {

            // Only needed when we have followers
            final int numFollowers = this.followerMap.size();
            if (numFollowers == 0)
                return;

            // Get all followers' leader timestamps, sorted in increasing order
            final Timestamp[] leaderTimestamps = new Timestamp[numFollowers];
            int index = 0;
            for (Follower follower : this.followerMap.values())
                leaderTimestamps[index++] = follower.getLeaderTimestamp();
            Arrays.sort(leaderTimestamps);

            //
            // Calculate highest leaderTimeout shared by a majority of followers (where "majority" includes myself).
            // Examples:
            //
            //  # nodes    self   followers
            //  -------    ----   ---------
            //     5       [x]    [ ][ ][x][x]        3/5 x's make a majority
            //     6       [x]    [ ][ ][x][x][x]     4/6 x's make a majority
            //
            // Odd or even, the leaderTimeout shared by a majority of nodes is at index leaderTimestamps.length / 2.
            // We then add the minimum election timeout, then subtract a little for clock drift.
            //
            final Timestamp newLeaseTimeout = leaderTimestamps[leaderTimestamps.length / 2]
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
                        follower.getUpdateTimer().timeoutNow();         // notify follower so it can commit waiting transaction(s)
                        timeouts.clear();
                    }
                }
            }
        }

        private void checkUpdateFollower(Follower follower) {

            // If follower has an in-progress snapshot that has become too stale, abort it
            SnapshotTransmit snapshotTransmit = follower.getSnapshotTransmit();
            if (snapshotTransmit != null && snapshotTransmit.getSnapshotIndex() < this.raft.lastAppliedIndex) {
                if (this.log.isDebugEnabled())
                    this.debug("aborting stale snapshot install for " + follower);
                follower.cancelSnapshotTransmit();
                follower.getUpdateTimer().timeoutNow();
            }

            // Is follower's queue empty? If not, hold off until then
            final String peer = follower.getIdentity();
            if (!this.raft.writablePeers.contains(peer)) {
                if (this.log.isTraceEnabled())
                    this.trace("no update for \"" + follower.getIdentity() + "\": output queue still not empty");
                return;
            }

            // Handle any in-progress snapshot install
            if ((snapshotTransmit = follower.getSnapshotTransmit()) != null) {

                // Send the next chunk in transmission, if any
                final long pairIndex = snapshotTransmit.getPairIndex();
                final ByteBuffer buf = snapshotTransmit.getNextChunk();
                boolean synced = true;
                if (buf != null) {

                    // Send next chunk
                    final InstallSnapshot msg = new InstallSnapshot(this.raft.identity, peer, this.raft.currentTerm,
                      snapshotTransmit.getSnapshotTerm(), snapshotTransmit.getSnapshotIndex(), pairIndex,
                      !snapshotTransmit.hasMoreChunks(), buf);
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
                follower.getUpdateTimer().timeoutNow();
            }

            // Are we still waiting for the update timer to expire?
            if (!follower.getUpdateTimer().needsService()) {
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
                final CloseableKVStore snapshot = this.raft.kv.snapshot();
                follower.setSnapshotTransmit(new SnapshotTransmit(this.raft.lastAppliedTerm,
                  this.raft.lastAppliedIndex, snapshot, STATE_MACHINE_PREFIX));
                this.info("started snapshot install for out-of-date " + follower);
                this.raft.requestService(new FollowerUpdateService(follower));
                return;
            }

            // Restart update timer here (to avoid looping if an error occurs below)
            follower.getUpdateTimer().timeoutAfter(this.raft.heartbeatTimeout);

            // Send actual data if follower is synced and there is a log entry to send; otherwise, just send a probe
            final AppendRequest msg;
            if (!follower.isSynced() || nextIndex > this.raft.getLastLogIndex()) {
                msg = new AppendRequest(this.raft.identity, peer, this.raft.currentTerm, new Timestamp(), this.leaseTimeout,
                  this.raft.commitIndex, this.raft.getLogTermAtIndex(nextIndex - 1), nextIndex - 1);     // send probe only
            } else {

                // Get log entry to send
                final LogEntry logEntry = this.raft.getLogEntryAtIndex(nextIndex);

                // If the log entry correspond's to follower's transaction, don't send the data because follower already has it.
                // But only do this optimization the first time, in case something goes wrong on the follower's end.
                ByteBuffer writesData = null;
                if (!follower.getSkipDataLogEntries().remove(logEntry)) {
                    try {
                        writesData = logEntry.getContent();
                    } catch (IOException e) {
                        this.error("error reading log file " + logEntry.getFile(), e);
                        return;
                    }
                }

                // Create message
                msg = new AppendRequest(this.raft.identity, peer, this.raft.currentTerm, new Timestamp(), this.leaseTimeout,
                  this.raft.commitIndex, this.raft.getLogTermAtIndex(nextIndex - 1), nextIndex - 1, logEntry.getTerm(), writesData);
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

        private class FollowerUpdateService extends Service {

            private final Follower follower;

            FollowerUpdateService(Follower follower) {
                super(LeaderRole.this, "update follower \"" + follower.getIdentity() + "\"");
                assert follower != null;
                this.follower = follower;
            }

            @Override
            public void run() {
                LeaderRole.this.checkUpdateFollower(this.follower);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null || obj.getClass() != this.getClass())
                    return false;
                final FollowerUpdateService that = (FollowerUpdateService)obj;
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
            if (writes.isEmpty()) {

                // Set commit term and index from last log entry
                tx.setCommitTerm(this.raft.getLastLogTerm());
                tx.setCommitIndex(this.raft.getLastLogIndex());
                if (this.log.isDebugEnabled()) {
                    this.debug("commit is " + tx.getCommitTerm() + "/" + tx.getCommitIndex()
                      + " for local read-only transaction " + tx);
                }

                // We will be able to commit this transaction immediately
                this.raft.requestService(new CheckWaitingTransactionService(tx));
            } else {

                // Append a new entry to the Raft log
                final LogEntry logEntry;
                try {
                    logEntry = this.raft.appendLogEntry(this.raft.currentTerm, tx.getMutableView().getWrites());
                } catch (Exception e) {
                    throw new KVTransactionException(tx, "error attempting to persist transaction", e);
                }
                if (this.log.isDebugEnabled())
                    this.debug("added log entry " + logEntry + " for local transaction " + tx);

                // Set commit term and index from new log entry
                tx.setCommitTerm(logEntry.getTerm());
                tx.setCommitIndex(logEntry.getIndex());

                // Prune applied log entries (check memory limit)
                this.raft.requestService(this.pruneAppliedLogEntriesService);

                // Update commit index (this is only needed in the single node case)
                if (this.followerMap.isEmpty())
                    this.raft.requestService(this.updateLeaderCommitIndexService);

                // Update all followers
                for (Follower follower : this.followerMap.values())
                    this.raft.requestService(new FollowerUpdateService(follower));
            }

            // Update transaction state
            tx.setState(TxState.COMMIT_WAITING);
        }

        @Override
        public void cleanupForTransaction(RaftKVTransaction tx) {
            // nothing to do
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
            }

            // Check result and update follower match index
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
                this.raft.requestService(new FollowerUpdateService(follower));
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
                this.raft.sendMessage(new CommitResponse(this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getTxId(), "error decoding reads data: " + e));
                return;
            }

            // Check for conflict
            final String conflictMsg = this.checkConflicts(msg.getBaseTerm(), msg.getBaseIndex(), reads);
            if (conflictMsg != null) {
                if (this.log.isDebugEnabled())
                    this.debug("commit request " + msg + " failed due to conflict: " + conflictMsg);
                this.raft.sendMessage(new CommitResponse(this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getTxId(), conflictMsg));
                return;
            }

            // Handle read-only vs. read-write transaction
            if (msg.isReadOnly()) {

                // Get current time
                final Timestamp minimumLeaseTimeout = new Timestamp();

                // If no other leader could have been elected yet, the transaction may be committed immediately
                final CommitResponse response;
                if (this.leaseTimeout.compareTo(minimumLeaseTimeout) > 0) {

                    // Follower may commit as soon as it sees that the most recent log entry has been committed
                    response = new CommitResponse(this.raft.identity, msg.getSenderId(), this.raft.currentTerm,
                      msg.getTxId(), this.raft.getLastLogTerm(), this.raft.getLastLogIndex());
                } else {

                    // Remember that this follower is now going to be waiting for this particular leaseTimeout
                    follower.getCommitLeaseTimeouts().add(minimumLeaseTimeout);

                    // Send immediate probes to all followers in an attempt to increase our leaseTimeout
                    for (Follower follower2 : this.followerMap.values())
                        follower2.getUpdateTimer().timeoutNow();

                    // Build response
                    response = new CommitResponse(this.raft.identity, msg.getSenderId(), this.raft.currentTerm,
                      msg.getTxId(), this.raft.getLastLogTerm(), this.raft.getLastLogIndex(), minimumLeaseTimeout);
                }

                // Send response
                this.raft.sendMessage(response);
            } else {

                // Append new entry to the Raft log
                final LogEntry logEntry;
                try {
                    logEntry = this.raft.appendLogEntry(this.raft.currentTerm, msg.getWritesData());
                } catch (Exception e) {
                    this.error("error appending new log entry for " + msg, e);
                    this.raft.sendMessage(new CommitResponse(this.raft.identity, msg.getSenderId(),
                      this.raft.currentTerm, msg.getTxId(), "error while attempting to persist transaction: " + e));
                    return;
                }
                if (this.log.isDebugEnabled())
                    this.debug("added log entry " + logEntry + " for remote " + msg);

                // Prune applied log entries (check memory limit)
                this.raft.requestService(this.pruneAppliedLogEntriesService);

                // Follower transaction data optimization
                follower.getSkipDataLogEntries().add(logEntry);

                // Update all followers
                for (Follower follower2 : this.followerMap.values())
                    this.raft.requestService(new FollowerUpdateService(follower2));

                // Send response
                this.raft.sendMessage(new CommitResponse(this.raft.identity, msg.getSenderId(),
                  this.raft.currentTerm, msg.getTxId(), this.raft.getLastLogTerm(), this.raft.getLastLogIndex()));
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

            // Too late dude, i already won the election
            if (this.log.isDebugEnabled())
                this.debug("ignoring " + msg + " rec'd while in " + this);
        }

        @Override
        public void caseGrantVote(GrantVote msg) {

            // Thanks and all, but i already won the election
            if (this.log.isDebugEnabled())
                this.debug("ignoring " + msg + " rec'd while in " + this);
        }

        private void failDuplicateLeader(Message msg) {

            // This should never happen - same term but two different leaders
            final boolean defer = this.raft.identity.compareTo(msg.getSenderId()) <= 0;
            this.error("detected a duplicate leader in " + msg + " - should never happen; possible inconsistent cluster"
              + " configuration on " + msg.getSenderId() + " (mine: " + this.raft.peerMap + "); "
              + (defer ? "reverting to follower" : "ignoring"));
            if (defer)
                this.raft.changeRole(new FollowerRole(this.raft, msg.getSenderId()));
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
            long index = this.raft.lastAppliedIndex - this.appliedLogEntries.size() + 1;
            for (LogEntry logEntry : Iterables.concat(this.appliedLogEntries, this.raft.raftLog))
                assert logEntry.getIndex() == index++;
            return true;
        }

    // Internal methods

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
                return "transaction is too old: snapshot index " + baseIndex + " < current state machine index " + minIndex;
            if (baseIndex > maxIndex)
                return "transaction is too new: snapshot index " + baseIndex + " > most recent log index " + maxIndex;

            // Validate the term of the log entry on which the transaction is based
            if (baseTerm != this.raft.getLogTermAtIndex(baseIndex)) {
                return "transaction is based on an overwritten log entry with index "
                  + baseIndex + " and term " + baseTerm + " != " + this.raft.getLogTermAtIndex(baseIndex);
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
                this.warn("rec'd " + msg + " from unknown peer `" + msg.getSenderId() + "', ignoring");
            return follower;
        }
    }

    /**
     * Support superclass for {@link FollowerRole} and {@link CandidateRole}, which both have an election timer.
     */
    private abstract static class NonLeaderRole extends Role {

        private final Timer electionTimer = this.raft.new Timer("election timer", new Service(this, "election timeout") {
            @Override
            public void run() {
                NonLeaderRole.this.checkElectionTimeout();
            }
        });

    // Constructors

        protected NonLeaderRole(RaftKVDatabase raft) {
            super(raft);
        }

    // Lifecycle

        @Override
        public void setup() {
            super.setup();
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
            if (this.electionTimer.needsService()) {
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

    // Debug

        @Override
        boolean checkState() {
            if (!super.checkState())
                return false;
            assert this.electionTimer.isRunning();
            return true;
        }
    }

// FOLLOWER role

    private static class FollowerRole extends NonLeaderRole {

        private String leader;                                                          // our leader, if known
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
            this(raft, null, null);
        }

        public FollowerRole(RaftKVDatabase raft, String leader) {
            this(raft, leader, leader);
        }

        public FollowerRole(RaftKVDatabase raft, String leader, String votedFor) {
            super(raft);
            Preconditions.checkArgument(leader == null || this.raft.peerMap.containsKey(leader), "invalid leader: %s", leader);
            this.leader = leader;
            this.votedFor = votedFor;
        }

    // Lifecycle

        @Override
        public void setup() {
            super.setup();
            this.info("entering follower role in term " + this.raft.currentTerm
              + (this.leader != null ? "; with leader \"" + this.leader + "\"" : "")
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
        public void outputQueueEmpty(String peer) {
            if (peer.equals(this.leader))
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

            // If we don't have a leader yet, or leader's queue is full, we must wait
            if (this.leader == null || !this.raft.writablePeers.contains(this.leader)) {
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
            ByteBuffer writesData = null;
            if (!writes.isEmpty()) {

                // Serialize writes into a temporary file (but do not close or durably persist yet)
                final File file = new File(this.raft.logDir,
                  String.format("%s%019d%s", TX_FILE_PREFIX, tx.getTxId(), TEMP_FILE_SUFFIX));
                try {
                    writes.serialize((fileWriter = new FileWriter(file)));
                    fileWriter.flush();
                } catch (IOException e) {
                    fileWriter.getFile().delete();
                    Util.closeIfPossible(fileWriter);
                    throw new KVTransactionException(tx, "error saving transaction mutations to temporary file", e);
                }
                final long writeLength = fileWriter.getLength();

                // Load serialized writes from file
                try {
                    writesData = Util.readFile(fileWriter.getFile(), writeLength);
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
            final CommitRequest msg = new CommitRequest(this.raft.identity, this.leader,
              this.raft.currentTerm, tx.getTxId(), tx.getBaseTerm(), tx.getBaseIndex(), readsData, writesData);
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

            // Record leader
            if (!msg.getSenderId().equals(this.leader)) {
                if (this.leader != null && !this.leader.equals(msg.getSenderId())) {
                    this.error("detected a conflicting leader in " + msg + " (previous leader was \"" + this.leader
                      + "\") - should never happen; possible inconsistent cluster configuration (mine: " + this.raft.peerMap + ")");
                }
                this.leader = msg.getSenderId();
                this.leaderLeaseTimeout = msg.getLeaderLeaseTimeout();
                this.info("updated leader to \"" + this.leader + "\"");
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

            // Restart election timeout
            this.restartElectionTimer();

            // If a snapshot install is in progress, cancel it
            if (this.snapshotReceive != null) {
                this.info("rec'd " + msg + " during in-progress " + this.snapshotReceive
                  + "; aborting snapshot install and resetting state machine");
                this.raft.resetStateMachine(true);
                this.snapshotReceive = null;
            }

            // Get my last log entry's index and term
            long lastLogTerm = this.raft.getLastLogTerm();
            long lastLogIndex = this.raft.getLastLogIndex();

            // Check whether our previous log entry term matches that of leader; if not, or it doesn't exist, request fails
            if (leaderPrevIndex < this.raft.lastAppliedIndex
              || leaderPrevIndex > lastLogIndex
              || leaderPrevTerm != this.raft.getLogTermAtIndex(leaderPrevIndex)) {
                if (this.log.isDebugEnabled())
                    this.debug("rejecting " + msg + " because previous log entry doesn't match");
                this.raft.sendMessage(new AppendResponse(this.raft.identity, msg.getSenderId(), this.raft.currentTerm,
                  msg.getLeaderTimestamp(), false, this.raft.lastAppliedIndex, this.raft.getLastLogIndex()));
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
                        logEntry.getFile().delete();
                    }
                    try {
                        this.raft.logDirChannel.force(true);
                    } catch (IOException e) {
                        this.warn("errory fsync()'ing log directory " + this.raft.logDir, e);
                    }
                    conflictList.clear();

                    // Update last log entry info
                    lastLogTerm = this.raft.getLastLogTerm();
                    lastLogIndex = this.raft.getLastLogIndex();
                }

                // Append the new log entry - if we don't already have it
                if (logIndex > lastLogIndex) {
                    assert logIndex == lastLogIndex + 1;
                    success = false;
                    do {

                        // If message contains no data, we expect to get the data from the corresponding transaction
                        if (msg.getWritesData() == null) {

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
                                      + logTerm + "/" + logIndex + " found; rejecting");
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
                                this.raft.appendLogEntry(logTerm,
                                  tx.getMutableView().getWrites(), pendingWrite.getFileWriter().getFile());
                            } catch (Exception e) {
                                this.error("error appending new log entry for " + tx, e);
                                pendingWrite.cleanup();
                                break;
                            }

                            // Debug
                            if (this.log.isDebugEnabled()) {
                                this.debug("now waiting for commit of " + tx.getCommitTerm() + "/" + tx.getCommitIndex()
                                  + " to commit " + tx);
                            }
                        } else {

                            // Append new log entry normally using the data from the request
                            try {
                                this.raft.appendLogEntry(logTerm, msg.getWritesData());
                            } catch (Exception e) {
                                this.error("error appending new log entry", e);
                                break;
                            }
                        }
                        success = true;
                    } while (false);

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
                  + " lastApplied=" + this.raft.lastAppliedTerm + "/" + this.raft.lastAppliedIndex
                  + " log=" + this.raft.raftLog);
            }

            // Send reply
            if (success) {
                this.raft.sendMessage(new AppendResponse(this.raft.identity, msg.getSenderId(), this.raft.currentTerm,
                  msg.getLeaderTimestamp(), true, msg.isProbe() ? logIndex - 1 : logIndex, this.raft.getLastLogIndex()));
            } else {
                this.raft.sendMessage(new AppendResponse(this.raft.identity, msg.getSenderId(), this.raft.currentTerm,
                  msg.getLeaderTimestamp(), false, this.raft.lastAppliedIndex, this.raft.getLastLogIndex()));
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
                this.raft.requestService(new CheckWaitingTransactionService(tx));
            } else
                this.raft.fail(tx, new RetryTransactionException(tx, msg.getErrorMessage()));
        }

        @Override
        public void caseInstallSnapshot(InstallSnapshot msg) {

            // Restart election timer
            this.restartElectionTimer();

            // Get snapshot term and index
            final long term = msg.getSnapshotTerm();
            final long index = msg.getSnapshotIndex();

            // Do we have an existing install?
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
                    this.snapshotReceive = null;
                }
            } else {

                // If the message is NOT the first one in a new install, ignore it
                if (msg.getPairIndex() != 0) {
                    this.info("rec'd non-initial " + msg + " with no in-progress snapshot install; ignoring");
                    return;
                }
            }

            // Set up new install if necessary
            if (this.snapshotReceive == null) {
                assert msg.getPairIndex() == 0;
                if (!this.raft.resetStateMachine(false))
                    return;
                this.snapshotReceive = new SnapshotReceive(PrefixKVStore.create(this.raft.kv, STATE_MACHINE_PREFIX), term, index);
                this.info("starting new snapshot install from \"" + msg.getSenderId() + "\" of " + term + "/" + index);
            }
            assert this.snapshotReceive.matches(msg);

            // Apply next chunk of key/value pairs
            if (this.log.isDebugEnabled())
                this.debug("applying " + msg + " to " + this.snapshotReceive);
            try {
                this.snapshotReceive.applyNextChunk(msg.getData());
            } catch (Exception e) {
                this.error("error applying snapshot to key/value store; resetting state machine", e);
                this.raft.resetStateMachine(true);
                this.snapshotReceive = null;
                return;
            }

            // If that was the last chunk, finalize persistent state
            if (msg.isLastChunk()) {
                this.info("snapshot install from \"" + msg.getSenderId() + "\" of " + term + "/" + index + " complete");
                this.snapshotReceive = null;
                if (!this.raft.updateStateMachine(term, index))
                    this.raft.resetStateMachine(true);
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
                this.info("rec'd " + msg + "; rejected because their log " + msg.getLastLogTerm() + "/"
                  + msg.getLastLogIndex() + " loses to ours " + this.raft.getLastLogTerm() + "/" + this.raft.getLastLogIndex());
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
            this.raft.sendMessage(new GrantVote(this.raft.identity, peer, this.raft.currentTerm));
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
              + "]";
        }

    // Debug

        @Override
        boolean checkState() {
            if (!super.checkState())
                return false;
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
        // will  have null writesData, because we will already have the data on hand waiting in a temporary file. This
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
            super(raft);
        }

    // Lifecycle

        @Override
        public void setup() {
            super.setup();

            // Increment term
            if (!this.raft.advanceTerm(this.raft.currentTerm + 1))
                return;

            // Request votes from peers
            final Set<String> peers = this.raft.peerMap.keySet();
            this.info("entering candidate role in term " + this.raft.currentTerm + "; requesting votes from " + peers);
            for (String peer : peers) {
                this.raft.sendMessage(new RequestVote(this.raft.identity, peer,
                  this.raft.currentTerm, this.raft.getLastLogTerm(), this.raft.getLastLogIndex()));
            }
        }

    // Service

        @Override
        public void outputQueueEmpty(String peer) {
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
            this.raft.changeRole(new FollowerRole(this.raft, msg.getSenderId()));
            this.raft.receiveMessage(msg);
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
            final int allVotes = 1 + this.raft.peerMap.size();                  // note: followerMap does not include myself
            final int numVotes = 1 + this.votes.size();                         // count myself plus all peer votes
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
            assert this.raftLog.isEmpty();
            assert this.logDirChannel == null;
            assert this.executor == null;
            assert this.writablePeers.isEmpty();
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

        // Check role
        assert this.role.checkState();

        // Check transactions
        for (RaftKVTransaction tx : this.openTransactions.values())
            tx.checkStateOpen(this.currentTerm, this.getLastLogIndex(), this.commitIndex);
    }
}

