
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.dellroad.stuff.java.TimedWait;
import org.dellroad.stuff.net.Network;
import org.dellroad.stuff.net.TCPNetwork;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.mvcc.AtomicKVStore;
import org.jsimpledb.kv.mvcc.Writes;
import org.jsimpledb.kv.raft.msg.AppendRequest;
import org.jsimpledb.kv.raft.msg.AppendResponse;
import org.jsimpledb.kv.raft.msg.CommitRequest;
import org.jsimpledb.kv.raft.msg.CommitResponse;
import org.jsimpledb.kv.raft.msg.GrantVote;
import org.jsimpledb.kv.raft.msg.InstallSnapshot;
import org.jsimpledb.kv.raft.msg.Message;
import org.jsimpledb.kv.raft.msg.MessageSwitch;
import org.jsimpledb.kv.raft.msg.PingRequest;
import org.jsimpledb.kv.raft.msg.PingResponse;
import org.jsimpledb.kv.raft.msg.RequestVote;
import org.jsimpledb.kv.util.KeyWatchTracker;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.LongEncoder;
import org.jsimpledb.util.ThrowableUtil;
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
 * {@link RaftKVDatabase} turns this into a transactional key/value database with linearizable ACID semantics.
 *
 * <p><b>Implementation Details</b></p>
 *
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
 *      <li>Transactions execute locally until commit time, using a {@link org.jsimpledb.kv.mvcc.MutableView} to collect mutations.
 *          The {@link org.jsimpledb.kv.mvcc.MutableView} is based on the local node's most recent log entry
 *          (whether committed or not); this is called the <i>base term and index</i> for the transaction.</li>
 *      <li>On commit, the transaction's {@link org.jsimpledb.kv.mvcc.Reads}, {@link org.jsimpledb.kv.mvcc.Writes},
 *          base index and term, and any config change are {@linkplain CommitRequest sent} to the leader.</li>
 *      <li>The leader confirms that the log entry corresponding to the transaction's base index is either not yet applied,
 *          or was its most recently applied log entry. If this is not the case, then the transaction's base log entry
 *          is too old (older than <i>T<sub>max</sub></i>, or was applied and discarded early due to memory pressure),
 *          and so the transaction is rejected with a {@link RetryTransactionException}.
 *      <li>The leader verifies that the the log entry term matches the transaction's base term; if not, the base log entry
 *          has been overwritten, and the transaction is rejected with a {@link RetryTransactionException}.
 *      <li>The leader confirms that the {@link Writes} associated with log entries after the transaction's base log entry
 *          do not create {@linkplain org.jsimpledb.kv.mvcc.Reads#isConflict conflicts} when compared against the transaction's
 *          {@link org.jsimpledb.kv.mvcc.Reads}. If so, the transaction is rejected with a {@link RetryTransactionException}.</li>
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
 *  <li>Optional weaker consistency guarantees are availble on a per-transaction bases; see
 *      {@link RaftKVTransaction#setConsistency RaftKVTransaction.setConsistency()}.</li>
 *  </ul>
 *
 * <p><b>Limitations</b></p>
 *
 * <ul>
 *  <li>A transaction's mutations must fit in memory.</li>
 *  <li>An {@link AtomicKVStore} is required to store local persistent state.</li>
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
 * and they disallow local transactions that make any changes other than as described below to create a new cluster.
 *
 * <p>
 * An unconfigured node becomes configured when either:
 * <ol>
 *  <li>{@link RaftKVTransaction#configChange RaftKVTransaction.configChange()} is invoked and committed within
 *      a local transaction, which creates a new single node cluster and commits the first log entry; or</li>
 *  <li>An {@link AppendRequest} is received from a leader of some existing cluster, in which case the node
 *      records the cluster ID thereby joining the cluster (see below), and applies the received cluster configuration.</li>
 * </ol>
 *
 * <p>
 * A node is configured if and only if it has recorded one or more log entries. The very first log entry
 * always contains the initial cluster configuration (containing only the node that created it, whether local or remote),
 * so any node that has a non-empty log is configured.
 *
 * <p>
 * Newly created clusters are assigned a random 32-bit cluster ID (option #1 above). This ID is included in all messages sent
 * over the network, and adopted by unconfigured nodes that join the cluster (via option #2 above). Configured nodes discard
 * incoming messages containing a cluster ID different from the one they have joined. This prevents data corruption that can
 * occur if nodes from two different clusters are inadvertently "mixed" together.
 *
 * <p>
 * Once a node joins a cluster with a specific cluster ID, it cannot be reassigned to a different cluster without first
 * returning it to the unconfigured state; to do that, it must be shut it down and its persistent state deleted.
 *
 * <p><b>Configuration Changes</b></p>
 *
 * <p>
 * Once a node is configured, a separate issue is whether the node is <i>included</i> in its own configuration, i.e., whether
 * the node is a member of its cluster according to the current cluster configuration. A node that is not a member of its
 * cluster does not count its own vote to determine committed log entries (if a leader), and does not start elections
 * (if a follower). However, it will accept and respond to incoming {@link AppendRequest}s and {@link RequestVote}s.
 *
 * <p>
 * In addition, leaders follow these rules with respect to configuration changes:
 * <ul>
 *  <li>If a leader is removed from a cluster, it remains the leader until the corresponding configuration change
 *      is committed (not counting its own vote), and then steps down (i.e., reverts to follower).</li>
 *  <li>If a follower is added to a cluster, the leader immediately starts sending that follower {@link AppendRequest}s.</li>
 *  <li>If a follower is removed from a cluster, the leader continues to send that follower {@link AppendRequest}s
 *      until the follower acknowledges receipt of the log entry containing the configuration change.</li>
 *  <li>Leaders defer configuration changes until they have committed at least one log entry in the current term
 *      (see <a href="https://groups.google.com/d/msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J">this discussion</a>).</li>
 *  <li>Configuration changes that remove the last node in a cluster are disallowed.</li>
 *  <li>Only one configuration change may take place at a time.</li>
 * </ul>
 *
 * <p><b>Follower Probes</b></p>
 *
 * <p>
 * This implementation includes a modification to the Raft state machine to avoid unnecessary, disruptive elections
 * when a node or nodes is disconnected from, and then reconnected to, the majority.
 *
 * <p>
 * When a follower's election timeout fires, before converting into a candidate, the follower is required to verify
 * communication with a majority of the cluster using {@linkplain PingRequest} messages. Only when the follower has
 * successfully done so may it become a candidate.  While in this intermediate "probing" mode, the follower responds
 * normally to incoming messages. In particular, if the follower receives a valid {@link AppendRequest} from the leader, it
 * reverts back to normal operation.
 *
 * <p>
 * This behavior is optional, but enabled by default (see {@link #setFollowerProbingEnabled setFollowerProbingEnabled()});
 *
 * <p><b>Key Watches</b></p>
 *
 * <p>
 * {@linkplain RaftKVTransaction#watchKey Key watches} and {@linkplain RaftKVTransaction#mutableSnapshot mutable snapshots}
 * are supported.
 *
 * <p><b>Spring Isolation Levels</b></p>
 *
 * <p>
 * In Spring applications, the transaction {@link Consistency} level may be configured through the Spring
 * {@link org.jsimpledb.spring.JSimpleDBTransactionManager} by (ab)using the transaction isolation level setting,
 * for example, via the {@link org.springframework.transaction.annotation.Transactional &#64;Transactional} annotation's
 * {@link org.springframework.transaction.annotation.Transactional#isolation isolation()} property.
 * All Raft consistency levels are made available this way, though the mapping from Spring's isolation levels to
 * {@link RaftKVDatabase}'s consistency levels is only semantically approximate:
 *
 * <div style="margin-left: 20px;">
 * <table border="1" cellpadding="3" cellspacing="0" summary="Isolation Level Mapping">
 * <tr style="bgcolor:#ccffcc">
 *  <th align="left">Spring isolation level</th>
 *  <th align="left">{@link RaftKVDatabase} consistency level</th>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#DEFAULT}</td>
 *  <td>{@link Consistency#LINEARIZABLE}</td>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#SERIALIZABLE}</td>
 *  <td>{@link Consistency#LINEARIZABLE}</td>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#REPEATABLE_READ}</td>
 *  <td>{@link Consistency#EVENTUAL}</td>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#READ_COMMITTED}</td>
 *  <td>{@link Consistency#EVENTUAL_COMMITTED}</td>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#READ_UNCOMMITTED}</td>
 *  <td>{@link Consistency#UNCOMMITTED}</td>
 * </tr>
 * </table>
 * </div>
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
     * Default maximum supported applied log entry memory usage ({@value DEFAULT_MAX_UNAPPLIED_LOG_MEMORY} bytes).
     *
     * @see #setMaxUnappliedLogMemory
     */
    public static final long DEFAULT_MAX_UNAPPLIED_LOG_MEMORY = 100 * 1024 * 1024;                // 100MB

    /**
     * Default transaction commit timeout ({@value DEFAULT_COMMIT_TIMEOUT}).
     *
     * @see #setCommitTimeout
     * @see RaftKVTransaction#setTimeout
     */
    public static final int DEFAULT_COMMIT_TIMEOUT = 5000;                              // 5 seconds

    /**
     * Default TCP port ({@value #DEFAULT_TCP_PORT}) used to communicate with peers.
     */
    public static final int DEFAULT_TCP_PORT = 9660;

    /**
     * Option key for {@link #createTransaction(Map)}. Value should be a {@link Consistency} instance,
     * or the {@link Consistency#name name()} thereof.
     */
    public static final String OPTION_CONSISTENCY = "consistency";

    // Internal constants
    static final int MAX_SNAPSHOT_TRANSMIT_AGE = (int)TimeUnit.SECONDS.toMillis(90);    // 90 seconds
    static final int MAX_SLOW_FOLLOWER_APPLY_DELAY_HEARTBEATS = 10;
    static final int MAX_UNAPPLIED_LOG_ENTRIES = 64;
    static final int FOLLOWER_LINGER_HEARTBEATS = 3;                        // how long to keep updating removed followers
    static final float MAX_CLOCK_DRIFT = 0.01f;                             // max clock drift as a percentage ratio

    // File prefixes and suffixes
    static final String TX_FILE_PREFIX = "tx-";
    static final String TEMP_FILE_PREFIX = "temp-";
    static final String TEMP_FILE_SUFFIX = ".tmp";
    static final Pattern TEMP_FILE_PATTERN = Pattern.compile(".*" + Pattern.quote(TEMP_FILE_SUFFIX));

    // Keys for persistent Raft state
    static final byte[] CLUSTER_ID_KEY = ByteUtil.parse("0001");
    static final byte[] CURRENT_TERM_KEY = ByteUtil.parse("0002");
    static final byte[] LAST_APPLIED_TERM_KEY = ByteUtil.parse("0003");
    static final byte[] LAST_APPLIED_INDEX_KEY = ByteUtil.parse("0004");
    static final byte[] LAST_APPLIED_CONFIG_KEY = ByteUtil.parse("0005");
    static final byte[] VOTED_FOR_KEY = ByteUtil.parse("0006");
    static final byte[] FLIP_FLOP_KEY = ByteUtil.parse("0007");

    // Prefix for all state machine key/value keys (we alternate between these to handle snapshot installs)
    private static final byte[] STATE_MACHINE_PREFIXES = new byte[] { (byte)0x80, (byte)0x81 };

    // Logging
    final Logger log = LoggerFactory.getLogger(this.getClass());

    // Configuration state
    Network network = new TCPNetwork(DEFAULT_TCP_PORT);
    String identity;
    int minElectionTimeout = DEFAULT_MIN_ELECTION_TIMEOUT;
    int maxElectionTimeout = DEFAULT_MAX_ELECTION_TIMEOUT;
    int heartbeatTimeout = DEFAULT_HEARTBEAT_TIMEOUT;
    int maxTransactionDuration = DEFAULT_MAX_TRANSACTION_DURATION;
    int commitTimeout = DEFAULT_COMMIT_TIMEOUT;
    long maxUnappliedLogMemory = DEFAULT_MAX_UNAPPLIED_LOG_MEMORY;
    boolean followerProbingEnabled;
    File logDir;

    // Raft runtime state
    Role role;                                                          // Raft state: LEADER, FOLLOWER, or CANDIDATE
    SecureRandom random;                                                // used to randomize election timeout, etc.
    boolean flipflop;                                                   // determines which state machine prefix we are using
    int clusterId;                                                      // cluster ID (zero if unconfigured - usually)
    long currentTerm;                                                   // current Raft term (zero if unconfigured)
    long currentTermStartTime;                                          // timestamp of the start of the current Raft term
    long commitIndex;                                                   // current Raft commit index (zero if unconfigured)
    long keyWatchIndex;                                                 // index of last log entry that triggered key watches
    long lastAppliedTerm;                                               // key/value store last applied term (zero if unconfigured)
    long lastAppliedIndex;                                              // key/value store last applied index (zero if unconfigured)
    final ArrayList<LogEntry> raftLog = new ArrayList<>();              // unapplied log entries (empty if unconfigured)
    Map<String, String> lastAppliedConfig;                              // key/value store last applied config (empty if none)
    Map<String, String> currentConfig;                                  // most recent cluster config (empty if unconfigured)

    // Non-Raft runtime state
    AtomicKVStore kv;
    FileChannel logDirChannel;                                          // null on Windows - no support for sync'ing directories
    String returnAddress;                                               // return address for message currently being processed
    ScheduledExecutorService serviceExecutor;
    final HashSet<String> transmitting = new HashSet<>();               // network addresses whose output queues are not empty
    final HashMap<Long, RaftKVTransaction> openTransactions = new HashMap<>();
    final LinkedHashSet<Service> pendingService = new LinkedHashSet<>();
    KeyWatchTracker keyWatchTracker;                                    // instantiated on demand
    boolean performingService;
    boolean shuttingDown;                                               // prevents new transactions from being created

// Configuration

    /**
     * Configure the {@link AtomicKVStore} in which local persistent state is stored.
     *
     * <p>
     * Required property.
     *
     * @param kvstore local persistent data store
     * @throws IllegalStateException if this instance is already started
     */
    public synchronized void setKVStore(AtomicKVStore kvstore) {
        Preconditions.checkState(this.role == null, "already started");
        this.kv = kvstore;
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
     * Get the directory in which uncommitted log entries are stored.
     *
     * @return configured log directory
     */
    public synchronized File getLogDirectory() {
        return this.logDir;
    }

    /**
     * Configure the {@link Network} to use for inter-node communication.
     *
     * <p>
     * By default, a {@link TCPNetwork} instance communicating on {@link #DEFAULT_TCP_PORT} is used.
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
     * Required property.
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
     * Get the configured minimum election timeout.
     *
     * @return minimum election timeout in milliseconds
     */
    public synchronized int getMinElectionTimeout() {
        return this.minElectionTimeout;
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
     * Get the configured maximum election timeout.
     *
     * @return maximum election timeout in milliseconds
     */
    public synchronized int getMaxElectionTimeout() {
        return this.maxElectionTimeout;
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
     * Get the configured heartbeat timeout.
     *
     * @return heartbeat timeout in milliseconds
     */
    public synchronized int getHeartbeatTimeout() {
        return this.heartbeatTimeout;
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
     * @see #setMaxUnappliedLogMemory
     */
    public synchronized void setMaxTransactionDuration(int duration) {
        Preconditions.checkArgument(duration > 0, "duration <= 0");
        this.maxTransactionDuration = duration;
    }

    /**
     * Get the configured maximum supported duration for outstanding transactions.
     *
     * @return maximum supported duration for outstanding transactions in milliseconds
     */
    public synchronized int getMaxTransactionDuration() {
        return this.maxTransactionDuration;
    }

    /**
     * Configure the maximum allowed memory used for unapplied log entries.
     *
     * <p>
     * This value is the <i>M<sub>max</sub></i> value from the {@linkplain RaftKVDatabase overview}.
     * A higher value means transactions may be larger and/or stay open longer without causing a {@link RetryTransactionException}.
     *
     * <p>
     * This value is approximate, and only affects leaders; followers always apply committed log entries immediately.
     *
     * <p>
     * This value may be changed while this instance is already running.
     *
     * <p>
     * Default is {@link #DEFAULT_MAX_UNAPPLIED_LOG_MEMORY}.
     *
     * @param memory maximum allowed memory usage for cached applied log entries
     * @throws IllegalArgumentException if {@code memory <= 0}
     * @see #setMaxTransactionDuration
     */
    public synchronized void setMaxUnappliedLogMemory(long memory) {
        Preconditions.checkArgument(memory > 0, "memory <= 0");
        this.maxUnappliedLogMemory = memory;
    }

    /**
     * Get the configured maximum allowed memory used for unapplied log entries.
     *
     * @return maximum allowed memory usage for cached applied log entries
     */
    public synchronized long getMaxUnappliedLogMemory() {
        return this.maxUnappliedLogMemory;
    }

    /**
     * Configure the default transaction commit timeout.
     *
     * <p>
     * This value determines how transactions will wait once {@link RaftKVTransaction#commit commit()}
     * is invoked for the commit to succeed before failing with a {@link RetryTransactionException}.
     * This can be overridden on a per-transaction basis via {@link RaftKVTransaction#setTimeout}.
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
        this.commitTimeout = timeout;
    }

    /**
     * Get the configured default transaction commit timeout.
     *
     * @return transaction commit timeout in milliseconds, or zero for unlimited
     */
    public synchronized int getCommitTimeout() {
        return this.commitTimeout;
    }

    /**
     * Configure whether followers should be required to probe for network connectivity with a majority of the
     * cluster after an election timeout prior to becoming a candidate.
     *
     * <p>
     * This value may be changed at any time.
     *
     * <p>
     * The default is enabled.
     *
     * @param followerProbingEnabled true to enable, false to disable
     */
    public synchronized void setFollowerProbingEnabled(boolean followerProbingEnabled) {
        this.followerProbingEnabled = followerProbingEnabled;
    }

    /**
     * Determine whether follower probing prior to becoming a candidate is enabled.
     *
     * @return true if follower probing is enabled, otherwise false
     */
    public synchronized boolean isFollowerProbingEnabled() {
        return this.followerProbingEnabled;
    }

// Status

    /**
     * Retrieve the unique 32-bit ID for this node's cluster.
     *
     * <p>
     * A value of zero indicates an unconfigured system. Usually the reverse true, though an unconfigured system
     * can have a non-zero cluster ID in the rare case where an error occurred persisting the initial log entry.
     *
     * @return the unique ID of this node's cluster, or zero if this node is unconfigured
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
     * <p>
     * The returned map is a copy; changes have no effect on this instance.
     *
     * @return current configuration mapping from node identity to network address,
     *  or empty if this node is not started or unconfigured
     */
    public synchronized Map<String, String> getCurrentConfig() {
        return this.currentConfig != null ? new TreeMap<String, String>(this.currentConfig) : new TreeMap<String, String>();
    }

    /**
     * Determine whether this instance is configured.
     *
     * <p>
     * A node is configured if and only if it has at least one log entry. The first log entry always
     * includes a configuration change that adds the node that created it to the (previously empty) cluster.
     *
     * @return true if this instance is started and configured, otherwise false
     */
    public synchronized boolean isConfigured() {
        return this.lastAppliedIndex > 0 || !this.raftLog.isEmpty();
    }

    /**
     * Determine whether this node thinks that it is part of its cluster, as determined by its
     * {@linkplain #getCurrentConfig current configuration}.
     *
     * @return true if this instance is started and part of the cluster, otherwise false
     */
    public synchronized boolean isClusterMember() {
        return this.isClusterMember(this.identity);
    }

    /**
     * Determine whether this node thinks that the specified node is part of the cluster, as determined by its
     * {@linkplain #getCurrentConfig current configuration}.
     *
     * @param node node identity
     * @return true if this instance is started and the specified node is part of the cluster, otherwise false
     */
    public synchronized boolean isClusterMember(String node) {
        return this.currentConfig != null ? this.currentConfig.containsKey(node) : false;
    }

    /**
     * Get this instance's current role: leadeer, follower, or candidate.
     *
     * @return current {@link Role}, or null if not running
     */
    public synchronized Role getCurrentRole() {
        return this.role;
    }

    /**
     * Get this instance's current term.
     *
     * @return current term, or zero if not running
     */
    public synchronized long getCurrentTerm() {
        return this.currentTerm;
    }

    /**
     * Get the time at which this instance's current term advanced to its current value.
     *
     * @return current term's start time in milliseconds since the epoch, or zero if unknown
     */
    public synchronized long getCurrentTermStartTime() {
        return this.currentTermStartTime;
    }

    /**
     * Get this instance's current commit index..
     *
     * @return current commit index, or zero if not running
     */
    public synchronized long getCommitIndex() {
        return this.commitIndex;
    }

    /**
     * Get this instance's last applied log entry term.
     *
     * @return last applied term, or zero if not running
     */
    public synchronized long getLastAppliedTerm() {
        return this.lastAppliedTerm;
    }

    /**
     * Get this instance's last applied log entry index.
     *
     * @return last applied index, or zero if not running
     */
    public synchronized long getLastAppliedIndex() {
        return this.lastAppliedIndex;
    }

    /**
     * Get the unapplied {@link LogEntry}s in this instance's Raft log.
     *
     * <p>
     * The returned list is a copy; changes have no effect on this instance.
     *
     * @return unapplied log entries; or null if this instance is not running
     */
    public synchronized List<LogEntry> getUnappliedLog() {
        return this.raftLog != null ? new ArrayList<>(this.raftLog) : null;
    }

    /**
     * Get the estimated total memory used by unapplied log entries.
     *
     * @return unapplied log entry memory usage, or zero if this instance is not running
     */
    public synchronized long getUnappliedLogMemoryUsage() {
        long total = 0;
        for (LogEntry logEntry : this.raftLog)
            total += logEntry.getFileSize();
        return total;
    }

    /**
     * Get the set of open transactions associated with this database.
     *
     * <p>
     * The returned set is a copy; changes have no effect on this instance.
     *
     * @return all open transactions
     */
    public synchronized List<RaftKVTransaction> getOpenTransactions() {
        final ArrayList<RaftKVTransaction> list;
        synchronized (this) {
            list = new ArrayList<>(this.openTransactions.values());
        }
        Collections.sort(list, RaftKVTransaction.SORT_BY_ID);
        return list;
    }

// Lifecycle

    @Override
    @PostConstruct
    public synchronized void start() {

        // Sanity check
        assert this.checkState();
        if (this.role != null)
            return;
        Preconditions.checkState(!this.shuttingDown, "shutdown in progress");
        Preconditions.checkState(this.logDir != null, "no Raft log directory configured");
        Preconditions.checkState(this.kv != null, "no Raft local persistence key/value store configured");
        Preconditions.checkState(this.network != null, "no Raft network configured");
        Preconditions.checkState(this.minElectionTimeout <= this.maxElectionTimeout, "minElectionTimeout > maxElectionTimeout");
        Preconditions.checkState(this.heartbeatTimeout < this.minElectionTimeout, "heartbeatTimeout >= minElectionTimeout");
        Preconditions.checkState(this.identity != null, "no Raft identity configured");

        // Log
        if (this.log.isDebugEnabled())
            this.debug("starting " + this.getClass().getName() + " in directory " + this.logDir);

        // Start up local database
        boolean success = false;
        try {

            // Create/verify log directory
            if (!this.logDir.exists())
                Files.createDirectories(this.logDir.toPath());
            if (!this.logDir.isDirectory())
                throw new IOException("file `" + this.logDir + "' is not a directory");

            // Start k/v store
            this.kv.start();

            // Open directory containing log entry files so we have a way to fsync() it
            assert this.logDirChannel == null;
            try {
                this.logDirChannel = FileChannel.open(this.logDir.toPath());
            } catch (IOException e) {
                if (!this.isWindows())
                    throw e;
            }

            // Create randomizer
            assert this.random == null;
            this.random = new SecureRandom();

            // Start up service executor thread
            assert this.serviceExecutor == null;
            this.serviceExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable action) {
                    final Thread thread = new Thread(action);
                    thread.setName("RaftKVDatabase Service");
                    return thread;
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
            this.currentTermStartTime = System.currentTimeMillis();
            final String votedFor = this.decodeString(VOTED_FOR_KEY, null);
            this.lastAppliedTerm = this.decodeLong(LAST_APPLIED_TERM_KEY, 0);
            this.lastAppliedIndex = this.decodeLong(LAST_APPLIED_INDEX_KEY, 0);
            this.lastAppliedConfig = this.decodeConfig(LAST_APPLIED_CONFIG_KEY);
            this.flipflop = this.decodeBoolean(FLIP_FLOP_KEY);
            this.currentConfig = this.buildCurrentConfig();

            // If we crashed part way through a snapshot install, recover by discarding partial install
            if (this.discardFlipFloppedStateMachine() && this.log.isDebugEnabled())
                this.debug("detected partially applied snapshot install, discarding");

            // Initialize commit index and key watch index
            this.commitIndex = this.lastAppliedIndex;
            this.keyWatchIndex = this.commitIndex;

            // Reload outstanding log entries from disk
            this.loadLog();

            // Show recovered state
            if (this.log.isDebugEnabled()) {
                this.debug("recovered Raft state:"
                  + "\n  clusterId=" + (this.clusterId != 0 ? String.format("0x%08x", this.clusterId) : "none")
                  + "\n  currentTerm=" + this.currentTerm
                  + "\n  lastApplied=" + this.lastAppliedIndex + "t" + this.lastAppliedTerm
                  + "\n  lastAppliedConfig=" + this.lastAppliedConfig
                  + "\n  currentConfig=" + this.currentConfig
                  + "\n  votedFor=" + (votedFor != null ? "\"" + votedFor + "\"" : "nobody")
                  + "\n  log=" + this.raftLog);
            }

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
                Preconditions.checkArgument(this.getLastLogTerm() == 0);
                Preconditions.checkArgument(this.getLastLogIndex() == 0);
                Preconditions.checkArgument(this.currentConfig.isEmpty());
                Preconditions.checkArgument(this.raftLog.isEmpty());
            }

            // Start as follower (with unknown leader)
            this.changeRole(new FollowerRole(this, null, null, votedFor));

            // Done
            this.info("successfully started " + this + " in directory " + this.logDir);
            success = true;
        } catch (IOException e) {
            throw new RuntimeException("error starting up database", e);
        } finally {
            if (!success)
                this.cleanup();
        }

        // Sanity check
        assert this.checkState();
    }

    @Override
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

        // Shut down the service executor and wait for pending tasks to finish
        this.serviceExecutor.shutdownNow();
        try {
            this.serviceExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Final cleanup
        synchronized (this) {
            this.serviceExecutor = null;
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
        if (this.serviceExecutor != null) {
            this.serviceExecutor.shutdownNow();
            try {
                this.serviceExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            this.serviceExecutor = null;
        }
        this.kv.stop();
        Util.closeIfPossible(this.logDirChannel);
        this.logDirChannel = null;
        this.raftLog.clear();
        this.random = null;
        this.network.stop();
        this.currentTerm = 0;
        this.currentTermStartTime = 0;
        this.commitIndex = 0;
        this.keyWatchIndex = 0;
        this.clusterId = 0;
        this.lastAppliedTerm = 0;
        this.lastAppliedIndex = 0;
        this.lastAppliedConfig = null;
        this.currentConfig = null;
        if (this.keyWatchTracker != null) {
            this.keyWatchTracker.close();
            this.keyWatchTracker = null;
        }
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
        assert this.raftLog.isEmpty();

        // Scan for log entry files
        this.raftLog.clear();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(this.logDir.toPath())) {
            for (Path path : files) {
                final File file = path.toFile();

                // Ignore sub-directories (typically owned by the underlying k/v store)
                if (file.isDirectory())
                    continue;

                // Is this a log entry file?
                if (LogEntry.LOG_FILE_PATTERN.matcher(file.getName()).matches()) {
                    if (this.log.isDebugEnabled())
                        this.debug("recovering log file " + file.getName());
                    final LogEntry logEntry = LogEntry.fromFile(file);
                    this.raftLog.add(logEntry);
                    continue;
                }

                // Is this a leftover temporary file?
                if (TEMP_FILE_PATTERN.matcher(file.getName()).matches()) {
                    if (this.log.isDebugEnabled())
                        this.debug("deleting leftover temporary file " + file.getName());
                    Util.delete(file, "leftover temporary file");
                    continue;
                }

                // Unknown
                this.warn("ignoring unrecognized file " + file.getName() + " in my log directory");
            }
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
                Util.delete(logEntry.getFile(), "bogus log file");
                i.remove();
            } else {
                expectedIndex++;
                lastTermSeen = logEntry.getTerm();
            }
        }
        if (this.log.isDebugEnabled()) {
            this.debug("recovered " + this.raftLog.size() + " log entries: " + this.raftLog
              + " (" + this.getUnappliedLogMemoryUsage() + " total bytes)");
        }

        // Rebuild current configuration
        this.currentConfig = this.buildCurrentConfig();
    }

    /**
     * Reconstruct the current config by starting with the last applied config and applying
     * configuration deltas from unapplied log entries.
     */
    Map<String, String> buildCurrentConfig() {

        // Start with last applied config
        final HashMap<String, String> config = new HashMap<>(this.lastAppliedConfig);

        // Apply any changes found in uncommitted log entries
        for (LogEntry logEntry : this.raftLog)
            logEntry.applyConfigChange(config);

        // Done
        return config;
    }

// Key Watches

    synchronized ListenableFuture<Void> watchKey(RaftKVTransaction tx, byte[] key) {
        Preconditions.checkState(this.role != null, "not started");
        if (tx.getState() != TxState.EXECUTING)
            throw new StaleTransactionException(tx);
        if (this.keyWatchTracker == null)
            this.keyWatchTracker = new KeyWatchTracker();
        return this.keyWatchTracker.register(key);
    }

// Transactions

    /**
     * Create a new transaction.
     *
     * <p>
     * Equivalent to: {@link #createTransaction(Consistency) createTransaction}{@code (}{@link Consistency#LINEARIZABLE}{@code )}.
     *
     * @throws IllegalStateException if this instance is not {@linkplain #start started} or in the process of shutting down
     */
    @Override
    public RaftKVTransaction createTransaction() {
        return this.createTransaction(Consistency.LINEARIZABLE);
    }

    @Override
    public RaftKVTransaction createTransaction(Map<String, ?> options) {

        // Any options?
        if (options == null)
            return this.createTransaction(Consistency.LINEARIZABLE);

        // Look for options from the JSimpleDBTransactionManager
        Consistency consistency = null;
        Object isolation = options.get("org.springframework.transaction.annotation.Isolation");
        if (isolation instanceof Enum)
            isolation = ((Enum<?>)isolation).name();
        if (isolation != null) {
            switch (isolation.toString()) {
            case "READ_UNCOMMITTED":
                consistency = Consistency.UNCOMMITTED;
                break;
            case "READ_COMMITTED":
                consistency = Consistency.EVENTUAL_COMMITTED;
                break;
            case "REPEATABLE_READ":
                consistency = Consistency.EVENTUAL;
                break;
            case "SERIALIZABLE":
                consistency = Consistency.LINEARIZABLE;
                break;
            default:
                break;
            }
        }

        // Look for OPTION_CONSISTENCY option
        try {
            final Object value = options.get(OPTION_CONSISTENCY);
            if (value instanceof Consistency)
                consistency = (Consistency)value;
            else if (value instanceof String)
                consistency = Consistency.valueOf((String)value);
        } catch (Exception e) {
            // ignore
        }

        // Configure consistency level
        return this.createTransaction(consistency != null ? consistency : Consistency.LINEARIZABLE);
    }

    /**
     * Create a new transaction with the specified consistency.
     *
     * <p>
     * Transactions that wish to use {@link Consistency#EVENTUAL_COMMITTED} must be created using this method,
     * because the log entry on which the transaction is based is determined at creation time.
     *
     * @param consistency consistency level
     * @return newly created transaction
     * @throws IllegalArgumentException if {@code consistency} is null
     * @throws IllegalStateException if this instance is not {@linkplain #start started} or in the process of shutting down
     */
    public synchronized RaftKVTransaction createTransaction(Consistency consistency) {

        // Sanity check
        assert this.checkState();
        Preconditions.checkState(consistency != null, "null consistency");
        Preconditions.checkState(this.role != null, "not started");
        Preconditions.checkState(!this.shuttingDown, "shutting down");

        // Base transaction on the most recent log entry (if !committed). This is itself a form of optimistic locking: we assume
        // that the most recent log entry has a high probability of being committed (in the Raft sense), which is of course
        // required in order to commit any transaction based on it.
        final MostRecentView view = new MostRecentView(this, consistency.isBasedOnCommittedLogEntry());

        // Create transaction
        final RaftKVTransaction tx = new RaftKVTransaction(this,
          view.getTerm(), view.getIndex(), view.getSnapshot(), view.getView());
        tx.setConsistency(consistency);
        tx.setTimeout(this.commitTimeout);
        if (this.log.isDebugEnabled())
            this.debug("created new transaction " + tx);
        this.openTransactions.put(tx.getTxId(), tx);

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
                    tx.setState(TxState.COMMIT_READY);
                    this.requestService(new CheckReadyTransactionService(this.role, tx));

                    // Setup commit timer
                    final int timeout = tx.getTimeout();
                    if (timeout != 0) {
                        final Timer commitTimer = new Timer(this, "commit timer for " + tx,
                          new Service("commit timeout for tx#" + tx.getTxId()) {
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
                ThrowableUtil.prependCurrentStackTrace(cause);
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
        tx.getCommitFuture().set(null);
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
        tx.getCommitFuture().setException(e);
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
    void requestService(Service service) {
        assert Thread.holdsLock(this);
        assert service != null;
        if (!this.pendingService.add(service) || this.performingService)
            return;
        try {
            this.serviceExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    RaftKVDatabase.this.handlePendingService();
                }
            });
        } catch (RejectedExecutionException e) {
            if (!this.shuttingDown)
                this.warn("service executor task rejected, skipping", e);
        }
    }

    // Performs pending service requests (do not invoke directly)
    private synchronized void handlePendingService() {

        // Sanity check
        assert this.checkState();
        if (this.role == null)
            return;

        // While there is work to do, do it
        this.performingService = true;
        try {
            while (!this.pendingService.isEmpty()) {
                final Iterator<Service> i = this.pendingService.iterator();
                final Service service = i.next();
                i.remove();
                assert service != null;
                assert service.getRole() == null || service.getRole() == this.role;
                if (this.log.isTraceEnabled())
                    this.trace("SERVICE [" + service + "] in " + this.role);
                try {
                    service.run();
                } catch (Throwable t) {
                    RaftKVDatabase.this.error("exception in " + service, t);
                }
            }
        } finally {
            this.performingService = false;
        }
    }

// Raft state

    /**
     * Discard all key/value pairs in the "flip-flopped" state machine, i.e., the one that we are not currently using.
     *
     * @return true if there was anything to remove, otherwise false
     */
    boolean discardFlipFloppedStateMachine() {
        final byte[] dirtyPrefix = this.getFlipFloppedStateMachinePrefix();
        final Iterator<KVPair> dirtyIterator = this.kv.getRange(dirtyPrefix, ByteUtil.getKeyAfterPrefix(dirtyPrefix), false);
        final boolean dirty = dirtyIterator.hasNext();
        Util.closeIfPossible(dirtyIterator);
        if (dirty)
            this.kv.removeRange(dirtyPrefix, ByteUtil.getKeyAfterPrefix(dirtyPrefix));
        return dirty;
    }

    /**
     * Perform a state machine flip-flop operation. Normally this would happen after a successful snapshot install.
     */
    boolean flipFlopStateMachine(long term, long index, Map<String, String> config) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert term >= 0;
        assert index >= 0;
        if (this.log.isDebugEnabled())
            this.debug("performing state machine flip-flop to " + index + "t" + term + " with config " + config);
        if (config == null)
            config = new HashMap<String, String>(0);

        // Prepare updates
        final Writes writes = new Writes();
        writes.getPuts().put(LAST_APPLIED_TERM_KEY, LongEncoder.encode(term));
        writes.getPuts().put(LAST_APPLIED_INDEX_KEY, LongEncoder.encode(index));
        writes.getPuts().put(LAST_APPLIED_CONFIG_KEY, this.encodeConfig(config));
        writes.getPuts().put(FLIP_FLOP_KEY, this.encodeBoolean(!this.flipflop));

        // Update persistent store
        try {
            this.kv.mutate(writes, true);
        } catch (Exception e) {
            this.error("flip-flop error updating key/value store term/index to " + index + "t" + term, e);
            return false;
        }

        // Delete all unapplied log files (no longer applicable)
        this.raftLog.clear();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(this.logDir.toPath())) {
            for (Path path : files) {
                final File file = path.toFile();
                if (LogEntry.LOG_FILE_PATTERN.matcher(file.getName()).matches())
                    Util.delete(file, "unapplied log file");
            }
        } catch (IOException e) {
            this.error("error deleting unapplied log files in " + this.logDir + " (ignoring)", e);
        }

        // Update in-memory copy of persistent state
        this.flipflop = !this.flipflop;
        this.lastAppliedTerm = term;
        this.lastAppliedIndex = index;
        this.lastAppliedConfig = config;
        this.commitIndex = this.lastAppliedIndex;
        final TreeMap<String, String> previousConfig = new TreeMap<>(this.currentConfig);
        this.currentConfig = this.buildCurrentConfig();
        if (!this.currentConfig.equals(previousConfig))
            this.info("apply new cluster configuration after snapshot install: " + this.currentConfig);

        // Discard the flip-flopped state machine
        this.discardFlipFloppedStateMachine();

        // Trigger key watches
        this.requestService(this.role.triggerKeyWatchesService);

        // Done
        return true;
    }

    /**
     * Update and persist a new current term.
     */
    boolean advanceTerm(long newTerm) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert newTerm > this.currentTerm;
        if (this.log.isDebugEnabled())
            this.debug("advancing current term from " + this.currentTerm + " -> " + newTerm);

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
        this.currentTermStartTime = System.currentTimeMillis();
        return true;
    }

    /**
     * Join the specified cluster and persist the specified cluster ID.
     *
     * @param newClusterId cluster ID; must not be zero
     * @return true if successful, false if an error occurred
     * @throws IllegalStateException if this node is already part of some cluster
     * @throws IllegalArgumentException if {@code newClusterId} is zero
     */
    boolean joinCluster(int newClusterId) {

        // Sanity check
        assert Thread.holdsLock(this);
        Preconditions.checkArgument(newClusterId != 0);
        Preconditions.checkState(this.clusterId == 0);

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
     * Get the prefix for state machine we are currently using.
     */
    byte[] getStateMachinePrefix() {
        return this.getStateMachinePrefix(false);
    }

    /**
     * Get the prefix for the flip-flopped state machine.
     */
    byte[] getFlipFloppedStateMachinePrefix() {
        return this.getStateMachinePrefix(true);
    }

    private byte[] getStateMachinePrefix(boolean flipFlopped) {
        return new byte[] { STATE_MACHINE_PREFIXES[flipFlopped ^ this.flipflop ? 1 : 0] };
    }

    /**
     * Set the Raft role.
     *
     * @param role new role
     */
    void changeRole(Role role) {

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
        if (this.log.isDebugEnabled())
            this.debug("changing role to " + role);
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
    LogEntry appendLogEntry(long term, NewLogEntry newLogEntry) throws Exception {

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
        if (this.logDirChannel != null)
            this.logDirChannel.force(true);

        // Add new log entry to in-memory log
        this.raftLog.add(logEntry);

        // Update current config
        if (logEntry.applyConfigChange(this.currentConfig))
            this.info("applying new cluster configuration from log entry " + logEntry + ": " + this.currentConfig);

        // Done
        return logEntry;
    }

    long getLastLogIndex() {
        assert Thread.holdsLock(this);
        return this.lastAppliedIndex + this.raftLog.size();
    }

    long getLastLogTerm() {
        assert Thread.holdsLock(this);
        return this.getLogTermAtIndex(this.getLastLogIndex());
    }

    long getLogTermAtIndex(long index) {
        assert Thread.holdsLock(this);
        assert index >= this.lastAppliedIndex;
        assert index <= this.getLastLogIndex();
        return index == this.lastAppliedIndex ? this.lastAppliedTerm : this.getLogEntryAtIndex(index).getTerm();
    }

    LogEntry getLogEntryAtIndex(long index) {
        assert Thread.holdsLock(this);
        assert index > this.lastAppliedIndex;
        assert index <= this.getLastLogIndex();
        return this.raftLog.get((int)(index - this.lastAppliedIndex - 1));
    }

// Object

    @Override
    public synchronized String toString() {
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

    boolean isTransmitting(String address) {
        return this.transmitting.contains(address);
    }

// Messages

    synchronized boolean sendMessage(Message msg) {

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

    synchronized void receiveMessage(String address, Message msg) {

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
            if (this.log.isDebugEnabled()) {
                this.debug("rec'd " + msg.getClass().getSimpleName() + " with term " + msg.getTerm() + " > "
                  + this.currentTerm + " from \"" + peer + "\", updating term and "
                  + (this.role instanceof FollowerRole ? "remaining a" : "reverting to") + " follower");
            }
            if (!this.advanceTerm(msg.getTerm()))
                return;
            this.changeRole(msg.isLeaderMessage() ? new FollowerRole(this, peer, address) : new FollowerRole(this));
        }

        // Is sender's term too low? Ignore it (except ping request)
        if (msg.getTerm() < this.currentTerm && !(msg instanceof PingRequest)) {
            if (this.log.isDebugEnabled()) {
                this.debug("rec'd " + msg + " with term " + msg.getTerm() + " < " + this.currentTerm
                  + " from \"" + peer + "\" at " + address + ", ignoring");
            }
            return;
        }

        // Debug
        if (this.log.isTraceEnabled())
            this.trace("RECV " + msg + " in " + this.role + " from " + address);

        // Handle message
        this.returnAddress = address;
        try {
            msg.visit(new MessageSwitch() {
                @Override
                public void caseAppendRequest(AppendRequest msg) {
                    RaftKVDatabase.this.role.caseAppendRequest(msg);
                }
                @Override
                public void caseAppendResponse(AppendResponse msg) {
                    RaftKVDatabase.this.role.caseAppendResponse(msg);
                }
                @Override
                public void caseCommitRequest(CommitRequest msg) {
                    RaftKVDatabase.this.role.caseCommitRequest(msg);
                }
                @Override
                public void caseCommitResponse(CommitResponse msg) {
                    RaftKVDatabase.this.role.caseCommitResponse(msg);
                }
                @Override
                public void caseGrantVote(GrantVote msg) {
                    RaftKVDatabase.this.role.caseGrantVote(msg);
                }
                @Override
                public void caseInstallSnapshot(InstallSnapshot msg) {
                    RaftKVDatabase.this.role.caseInstallSnapshot(msg);
                }
                @Override
                public void casePingRequest(PingRequest msg) {
                    RaftKVDatabase.this.role.casePingRequest(msg);
                }
                @Override
                public void casePingResponse(PingResponse msg) {
                    RaftKVDatabase.this.role.casePingResponse(msg);
                }
                @Override
                public void caseRequestVote(RequestVote msg) {
                    RaftKVDatabase.this.role.caseRequestVote(msg);
                }
            });
        } finally {
            this.returnAddress = null;
        }
    }

// Utility methods

    byte[] encodeBoolean(boolean value) {
        return new byte[] { value ? (byte)1 : (byte)0 };
    }

    boolean decodeBoolean(byte[] key) throws IOException {
        final byte[] value = this.kv.get(key);
        return value != null && value.length > 0 && value[0] != 0;
    }

    long decodeLong(byte[] key, long defaultValue) throws IOException {
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

    String decodeString(byte[] key, String defaultValue) throws IOException {
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

    byte[] encodeString(String value) {
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

    Map<String, String> decodeConfig(byte[] key) throws IOException {
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

    byte[] encodeConfig(Map<String, String> config) {
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

    void trace(String msg, Throwable t) {
        this.log.trace(String.format("%s %s: %s", new Timestamp(), this.identity, msg), t);
    }

    void trace(String msg) {
        this.log.trace(String.format("%s %s: %s", new Timestamp(), this.identity, msg));
    }

    void debug(String msg, Throwable t) {
        this.log.debug(String.format("%s %s: %s", new Timestamp(), this.identity, msg), t);
    }

    void debug(String msg) {
        this.log.debug(String.format("%s %s: %s", new Timestamp(), this.identity, msg));
    }

    void info(String msg, Throwable t) {
        this.log.info(String.format("%s %s: %s", new Timestamp(), this.identity, msg), t);
    }

    void info(String msg) {
        this.log.info(String.format("%s %s: %s", new Timestamp(), this.identity, msg));
    }

    void warn(String msg, Throwable t) {
        this.log.warn(String.format("%s %s: %s", new Timestamp(), this.identity, msg), t);
    }

    void warn(String msg) {
        this.log.warn(String.format("%s %s: %s", new Timestamp(), this.identity, msg));
    }

    void error(String msg, Throwable t) {
        this.log.error(String.format("%s %s: %s", new Timestamp(), this.identity, msg), t);
    }

    void error(String msg) {
        this.log.error(String.format("%s %s: %s", new Timestamp(), this.identity, msg));
    }

// Debug/Sanity Checking

    private boolean checkState() {
        try {
            this.doCheckState();
        } catch (AssertionError e) {
            throw new AssertionError("checkState() failure for " + this, e);
        }
        return true;
    }

    private void doCheckState() {
        assert Thread.holdsLock(this);

        // Handle stopped state
        if (this.role == null) {
            assert this.random == null;
            assert this.currentTerm == 0;
            assert this.currentTermStartTime == 0;
            assert this.commitIndex == 0;
            assert this.lastAppliedTerm == 0;
            assert this.lastAppliedIndex == 0;
            assert this.lastAppliedConfig == null;
            assert this.currentConfig == null;
            assert this.clusterId == 0;
            assert this.raftLog.isEmpty();
            assert this.logDirChannel == null;
            assert this.serviceExecutor == null;
            assert this.keyWatchTracker == null;
            assert this.transmitting.isEmpty();
            assert this.openTransactions.isEmpty();
            assert this.pendingService.isEmpty();
            assert !this.shuttingDown;
            return;
        }

        // Handle running state
        assert this.kv != null;
        assert this.random != null;
        assert this.serviceExecutor != null;
        assert this.logDirChannel != null || this.isWindows();
        assert !this.serviceExecutor.isShutdown() || this.shuttingDown;

        assert this.currentTerm >= 0;
        assert this.commitIndex >= 0;
        assert this.lastAppliedTerm >= 0;
        assert this.lastAppliedIndex >= 0;
        assert this.lastAppliedConfig != null;
        assert this.currentConfig != null;

        assert this.currentTerm >= this.lastAppliedTerm;
        assert this.commitIndex >= this.lastAppliedIndex;
        assert this.commitIndex <= this.lastAppliedIndex + this.raftLog.size();
        assert this.keyWatchIndex <= this.commitIndex;
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

    private boolean isWindows() {
        return System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH).indexOf("win") != -1;
    }
}

