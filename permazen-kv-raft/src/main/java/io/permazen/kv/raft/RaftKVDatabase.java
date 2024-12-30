
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.KeyRange;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.Reads;
import io.permazen.kv.mvcc.SnapshotKVDatabase;
import io.permazen.kv.mvcc.TransactionConflictException;
import io.permazen.kv.mvcc.Writes;
import io.permazen.kv.raft.fallback.FallbackKVDatabase;
import io.permazen.kv.raft.msg.AppendRequest;
import io.permazen.kv.raft.msg.AppendResponse;
import io.permazen.kv.raft.msg.CommitRequest;
import io.permazen.kv.raft.msg.CommitResponse;
import io.permazen.kv.raft.msg.GrantVote;
import io.permazen.kv.raft.msg.InstallSnapshot;
import io.permazen.kv.raft.msg.Message;
import io.permazen.kv.raft.msg.MessageSwitch;
import io.permazen.kv.raft.msg.PingRequest;
import io.permazen.kv.raft.msg.PingResponse;
import io.permazen.kv.raft.msg.RequestVote;
import io.permazen.kv.util.KeyWatchTracker;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.LongEncoder;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.dellroad.stuff.io.ByteBufferInputStream;
import org.dellroad.stuff.java.ThrowableUtil;
import org.dellroad.stuff.java.TimedWait;
import org.dellroad.stuff.net.Network;
import org.dellroad.stuff.net.TCPNetwork;
import org.dellroad.stuff.util.LongMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A distributed {@link KVDatabase} based on the Raft consensus algorithm.
 *
 * <p><b>Raft Algorithm</b>
 *
 * <p>
 * Raft defines a distributed consensus algorithm for maintaining a shared state machine.
 * Each Raft node maintains a complete copy of the state machine. Cluster nodes elect a
 * leader who collects and distributes updates and provides for consistent reads.
 * As long as as a node is part of a majority, the state machine is fully operational.
 * Raft is described more fully <a href="https://raft.github.io/">here</a>.
 *
 * <p><b>Key/Value Database</b>
 *
 * <p>
 * {@link RaftKVDatabase} turns this into a transactional, highly available clustered key/value database with linearizable
 * (technically, <a href="https://jepsen.io/consistency/models/strict-serializable">strictly serializable</a>) consistency.
 * A {@link RaftKVDatabase} appears to each node in the cluster as a shared, ACID compliant key/value database.
 * As long as a node can communicate with a majority of other nodes (i.e., at least half of the cluster), then the
 * database is fully available.
 *
 * <p><b>Concurrent Transactions</b>
 *
 * <p>
 * {@link RaftKVDatabase} supports multiple simultaneous read/write transactions across all nodes. Simultaneous transactions
 * will successfully commit as long as the database is able to meet its consistency obligations. It does this verification
 * by performing conflict analysis at the key/value level. When two transactions do conflict, the loser receives a
 * {@link RetryKVTransactionException}. Because it is based on the Raft algorithm, consistency is guaranteed even in the face
 * of arbitrary network drops, delays, and reorderings.
 *
 * <p><b>Persistence</b>
 *
 * <p>
 * Each node maintains a complete copy of the database, and persistence is guaranteed even if up to half of the cluster
 * is lost. Each node stores its private persistent state in an {@link AtomicKVStore} (see {@link #setKVStore setKVStore()}).
 *
 * <p><b>Standalone Mode</b>
 *
 * <p>
 * Optional support for falling back to a "standalone mode" based on the most recent copy of the database when a majority of
 * nodes can't be reached is provided by {@link FallbackKVDatabase}.
 *
 * <p><b>Raft Implementation Details</b></p>
 *
 *  <ul>
 *  <li>The Raft state machine is the key/value store data.</li>
 *  <li>Unapplied log entries are stored on disk as serialized mutations, and also cached in memory.</li>
 *  <li>Concurrent transactions are supported through a simple optimistic locking MVCC scheme (similar to that used by
 *      {@link SnapshotKVDatabase}):
 *      <ul>
 *      <li>Transactions execute locally until commit time, using a {@link MutableView} to collect mutations.
 *          The {@link MutableView} is based on the local node's last unapplied log entry,
 *          if any (whether committed or not), or else directly on the underlying key/value store; this defines
 *          the <i>base term and index</i> for the transaction.</li>
 *      <li>Since the transaction's view incorporates all unapplied log entries down to the underlying
 *          compacted key/value store, transaction performance degrades as the number of unapplied log
 *          entries grows. Log entries are always applied as soon as possible, but they are also kept around
 *          on disk (up to a point) after being applied in case needed by a leader.</li>
 *      <li>On commit, the transaction's {@link Reads}, {@link Writes}, base index and term, and any config change are
 *          {@linkplain CommitRequest sent} to the leader.</li>
 *      <li>The leader confirms that the log entry corresponding to the transaction's base index and term matches its log.
 *          If this is not the case, then the transaction is rejected with a {@link TransactionConflictException}.
 *      <li>The leader confirms that the {@link Writes} associated with log entries (if any) after the transaction's base log entry
 *          do not create {@linkplain Reads#isConflict conflicts} when compared against the transaction's
 *          {@link Reads}. If so, the transaction is rejected with a {@link RetryKVTransactionException}.</li>
 *      <li>The leader adds a new log entry consisting of the transaction's {@link Writes} (and any config change) to its log.
 *          The associated term and index become the transaction's <i>commit term and index</i>; the leader then
 *          {@linkplain CommitResponse replies} to the follower with this information.</li>
 *      <li>If/when the follower sees a <i>committed</i> (in the Raft sense) log entry appear in its log matching the
 *          transaction's commit term and index, then the transaction is complete.</li>
 *      <li>As a small optimization, when the leader sends a log entry to the same follower who committed the corresponding
 *          transaction in the first place, only the transaction ID is sent, because the follower already has the data.</li>
 *      <li>After adding a new log entry, both followers and leaders "rebase" any open transactions by checking for conflicts
 *          in the manner described above. In this way, conflicts are detected as early as possible.</li>
 *      </ul>
 *  </li>
 *  <li>For transactions occurring on a leader, the logic is similar except of course the leader is talking to itself
 *      when it commits the transaction.</li>
 *  <li>For read-only transactions, the leader does not create a new log entry; instead, the transaction's commit
 *      term and index are set to the base term and index, and the leader also calculates its current "leader lease timeout",
 *      which is the earliest time at which it is possible for another leader to be elected.
 *      This is calculated as the time in the past at which the leader sent {@link AppendRequest}'s to a majority of followers
 *      who have since responded, plus the {@linkplain #setMinElectionTimeout minimum election timeout}, minus a small adjustment
 *      for possible clock drift (this assumes all nodes have the same minimum election timeout configured). If the current
 *      time is prior to the leader lease timeout, then the transaction may be committed as soon as the log entry
 *      corresponding to the commit term and index is committed (it may already be); otherwise, the current time is returned
 *      to the follower as minimum required leader lease timeout before the transaction may be committed.</li>
 *  <li>For read-only transactions, followers {@linkplain CommitRequest send} the base term and index to the leader as soon
 *      as the transaction is set read-only, without any conflict information. This allows the leader to capture and return
 *      the lowest possible commit index to the follower while the transaction is still open, and lets followers stop
 *      rebasing the transaction (at the returned commit index) as soon as possible, minimizing conflicts.
 *  <li>Every {@link AppendRequest} includes the leader's current timestamp and leader lease timeout, so followers can commit
 *      any waiting read-only transactions. Leaders keep track of which followers are waiting on which leader lease
 *      timeout values, and when the leader lease timeout advances to allow a follower to commit a transaction, the follower
 *      is immediately notified.</li>
 *  <li>Optional weaker consistency guarantees are availble on a per-transaction basis; see {@link #OPTION_CONSISTENCY}.
 *      Setting the consistency to any level other than {@link Consistency#LINEARIZABLE} implicitly sets the transaction
 *      to read-only.</li>
 *  </ul>
 *
 * <p><b>Limitations</b></p>
 *
 * <ul>
 *  <li>A transaction's mutations must fit in memory.</li>
 *  <li>All nodes must be configured with the same {@linkplain #setMinElectionTimeout minimum election timeout}.
 *      This guarantees that the leader's lease timeout calculation is valid.</li>
 *  <li>Due to the optimistic locking approach used, this implementation will perform poorly when there is a high
 *      rate of conflicting transactions; the result will be many transaction retries.</li>
 *  <li>Performance will suffer if the mutation data associated with a typical transaction cannot be delivered
 *      quickly and reliably over the network.</li>
 * </ul>
 *
 * <p>
 * In general, the algorithm should function correctly under all non-Byzantine conditions. The level of difficultly
 * the system is experiencing, due to contention, network errors, etc., can be measured in terms of:
 * <ul>
 *  <li>The average amount of time it takes to commit a transaction</li>
 *  <li>The frequency of {@link RetryKVTransactionException}'s</li>
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
 * An unconfigured node becomes <i>configured</i> when either:
 * <ol>
 *  <li>{@link RaftKVTransaction#configChange(String, String) RaftKVTransaction.configChange()} is invoked and committed
 *      within a local transaction, which creates a new single node cluster, with the current node as leader,
 *      and commits the cluster's first log entry; or</li>
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
 * occur if nodes from two different clusters are inadvertently mixed together on the same network.
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
 * own cluster does not count its own vote to determine committed log entries (if a leader), and does not start elections
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
 *  <li>Only one configuration change may take place at a time (i.e., be not yet committed).</li>
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
 * This behavior is optional, but enabled by default; see {@link #setFollowerProbingEnabled setFollowerProbingEnabled()}.
 *
 * <p><b>Key Watches</b></p>
 *
 * <p>
 * {@linkplain RaftKVTransaction#watchKey Key watches} are supported.
 *
 * <p><b>Snapshots</b></p>
 *
 * <p>
 * {@linkplain RaftKVTransaction#readOnlySnapshot Snapshots} are supported and can be created in constant time, because
 * with Raft every node maintains a complete copy of the database.
 *
 * <p><b>Spring Isolation Levels</b></p>
 *
 * <p>
 * In Spring applications, the transaction {@link Consistency} level may be configured through the Spring
 * {@link io.permazen.spring.PermazenTransactionManager} by (ab)using the transaction isolation level setting,
 * for example, via the {@link org.springframework.transaction.annotation.Transactional &#64;Transactional} annotation's
 * {@link org.springframework.transaction.annotation.Transactional#isolation isolation()} property.
 * All Raft consistency levels are made available this way, though the mapping from Spring's isolation levels to
 * {@link RaftKVDatabase}'s consistency levels is only semantically approximate:
 *
 * <div style="margin-left: 20px;">
 * <table class="striped">
 * <caption>Isolation Level Mapping</caption>
 * <tr style="bgcolor:#ccffcc">
 *  <th style="font-weight: bold; text-align: left">Spring isolation level</th>
 *  <th style="font-weight: bold; text-align: left">{@link RaftKVDatabase} consistency level</th>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#DEFAULT DEFAULT}</td>
 *  <td>{@link Consistency#LINEARIZABLE}</td>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#SERIALIZABLE SERIALIZABLE}</td>
 *  <td>{@link Consistency#LINEARIZABLE}</td>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#REPEATABLE_READ REPEATABLE_READ}</td>
 *  <td>{@link Consistency#EVENTUAL}</td>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#READ_COMMITTED READ_COMMITTED}</td>
 *  <td>{@link Consistency#EVENTUAL_COMMITTED}</td>
 * </tr>
 * <tr>
 *  <td>{@link org.springframework.transaction.annotation.Isolation#READ_UNCOMMITTED READ_UNCOMMITTED}</td>
 *  <td>{@link Consistency#UNCOMMITTED}</td>
 * </tr>
 * </table>
 * </div>
 *
 * <p><b>High Priority Transactions</b></p>
 *
 * <p>
 * Transactions may be configured as high priority; see
 * {@link RaftKVTransaction#setHighPriority RaftKVTransaction.setHighPriority()}.
 *
 * @see <a href="https://raftconsensus.github.io/">The Raft Consensus Algorithm</a>
 */
@ThreadSafe
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

    /**
     * Option key for {@link #createTransaction(Map)}. Value should be a Boolean, or else {@code "true"} or {@code "false"}.
     *
     * @see RaftKVTransaction#setHighPriority
     */
    public static final String OPTION_HIGH_PRIORITY = "highPriority";

    // Internal constants
    static final float MAX_CLOCK_DRIFT = 0.01f;                         // max clock drift per heartbeat as a percentage ratio

    // File prefixes and suffixes
    static final String TX_FILE_PREFIX = "tx-";
    static final String TEMP_FILE_PREFIX = "temp-";
    static final String TEMP_FILE_SUFFIX = ".tmp";
    static final Pattern TEMP_FILE_PATTERN = Pattern.compile(".*" + Pattern.quote(TEMP_FILE_SUFFIX));

    // Keys for persistent Raft state
    static final ByteData CLUSTER_ID_KEY = ByteData.fromHex("0001");
    static final ByteData CURRENT_TERM_KEY = ByteData.fromHex("0002");
    static final ByteData LAST_APPLIED_TERM_KEY = ByteData.fromHex("0003");
    static final ByteData LAST_APPLIED_INDEX_KEY = ByteData.fromHex("0004");
    static final ByteData LAST_APPLIED_CONFIG_KEY = ByteData.fromHex("0005");
    static final ByteData VOTED_FOR_KEY = ByteData.fromHex("0006");
    static final ByteData FLIP_FLOP_KEY = ByteData.fromHex("0007");

    // Prefix for all state machine key/value keys (we alternate between these to handle snapshot installs)
    private static final ByteData[] STATE_MACHINE_PREFIXES = new ByteData[] { ByteData.of(0x80), ByteData.of(0x81) };

    // Logging
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    // Configuration state
    @GuardedBy("this")
    Network network = new TCPNetwork(DEFAULT_TCP_PORT);
    @GuardedBy("this")
    String identity;
    @GuardedBy("this")
    int minElectionTimeout = DEFAULT_MIN_ELECTION_TIMEOUT;
    @GuardedBy("this")
    int maxElectionTimeout = DEFAULT_MAX_ELECTION_TIMEOUT;
    @GuardedBy("this")
    int heartbeatTimeout = DEFAULT_HEARTBEAT_TIMEOUT;
    @GuardedBy("this")
    int maxTransactionDuration = DEFAULT_MAX_TRANSACTION_DURATION;
    @GuardedBy("this")
    int commitTimeout = DEFAULT_COMMIT_TIMEOUT;
    @GuardedBy("this")
    int threadPriority = -1;
    @GuardedBy("this")
    boolean followerProbingEnabled;
    @GuardedBy("this")
    boolean disableSync;
    @GuardedBy("this")
    boolean dumpConflicts;
    @GuardedBy("this")
    File logDir;

    // Raft runtime state
    @GuardedBy("this")
    Role role;                                                          // Raft state: LEADER, FOLLOWER, or CANDIDATE
    @GuardedBy("this")
    SecureRandom random;                                                // used to randomize election timeout, etc.
    @GuardedBy("this")
    boolean flipflop;                                                   // determines which state machine prefix we are using
    @GuardedBy("this")
    int clusterId;                                                      // cluster ID (zero if unconfigured - usually)
    @GuardedBy("this")
    long currentTerm;                                                   // current Raft term (zero if unconfigured)
    @GuardedBy("this")
    long currentTermStartTime;                                          // timestamp of the start of the current Raft term
    @GuardedBy("this")
    long commitIndex;                                                   // current Raft commit index (zero if unconfigured)
    @GuardedBy("this")
    long keyWatchIndex;                                                 // index of last log entry that triggered key watches
    @GuardedBy("this")
    @SuppressWarnings("this-escape")
    final Log log = new Log(this);                                      // applied and unapplied log entries (empty if unconfigured)

    @GuardedBy("this")
    Map<String, String> currentConfig;                                  // most recent cluster config (empty if unconfigured)
    @GuardedBy("this")
    Map<String, Integer> protocolVersionMap = new HashMap<>();          // peer message encoding protocol versions

    // Non-Raft runtime state
    @GuardedBy("this")
    AtomicKVStore kv;
    @GuardedBy("this")
    FileChannel logDirChannel;                                          // null on Windows - no support for sync'ing directories
    @GuardedBy("this")
    String returnAddress;                                               // return address for message currently being processed
    @GuardedBy("this")
    IOThread ioThread;                                                  // performs background I/O tasks
    @GuardedBy("this")
    ScheduledExecutorService serviceExecutor;                           // does stuff for us asynchronously
    @GuardedBy("this")
    final HashSet<String> transmitting = new HashSet<>();               // network addresses whose output queues are not empty
    @GuardedBy("this")
    final LongMap<RaftKVTransaction> openTransactions = new LongMap<>();        // transactions open on this instance
    @GuardedBy("this")
    final LinkedHashSet<Service> pendingService = new LinkedHashSet<>();        // pending work for serviceExecutor
    @GuardedBy("this")
    KeyWatchTracker keyWatchTracker;                                    // instantiated on demand
    @GuardedBy("this")
    Timestamp linearizableCommitTimestamp;                              // time of most recent successful commit of linearizable tx
    @GuardedBy("this")
    boolean performingService;                                          // true when serviceExecutor does not need to be woken up
    @GuardedBy("this")
    boolean shuttingDown;                                               // prevents new transactions from being created
    @GuardedBy("this")
    Throwable lastInternalError;                                        // most recent exception in service executor

    // High priority transaction
    @GuardedBy("this")
    RaftKVTransaction highPrioTx;                                       // current high priority transaction, if any

    // Other
    volatile boolean performanceLogging = true;                         // performance-related log events at level INFO, not DEBUG

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
    public synchronized void setKVStore(final AtomicKVStore kvstore) {
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
    public synchronized void setLogDirectory(final File directory) {
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
    public synchronized void setNetwork(final Network network) {
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
    public synchronized void setIdentity(final String identity) {
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
    public synchronized void setMinElectionTimeout(final int timeout) {
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
    public synchronized void setMaxElectionTimeout(final int timeout) {
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
    public synchronized void setHeartbeatTimeout(final int timeout) {
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
     * This value may be changed while this instance is already running.
     *
     * <p>
     * Default is {@link #DEFAULT_MAX_TRANSACTION_DURATION}.
     *
     * @param duration maximum supported duration for outstanding transactions in milliseconds
     * @throws IllegalArgumentException if {@code duration <= 0}
     */
    public synchronized void setMaxTransactionDuration(final int duration) {
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
     * Configure the default transaction commit timeout.
     *
     * <p>
     * This value determines how transactions will wait once {@link RaftKVTransaction#commit commit()}
     * is invoked for the commit to succeed before failing with a {@link RetryKVTransactionException}.
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
    public synchronized void setCommitTimeout(final int timeout) {
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
    public synchronized void setFollowerProbingEnabled(final boolean followerProbingEnabled) {
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

    /**
     * Disable filesystem data sync.
     *
     * <p>
     * This gives higher performance in exchange for losing the guarantee of durability if the system crashes.
     * Note: this feature is experimental and may violate consistency and/or durability guaratees.
     *
     * <p>
     * Default is false.
     *
     * @param disableSync true to disable data sync
     */
    public synchronized void setDisableSync(final boolean disableSync) {
        this.disableSync = disableSync;
    }

    /**
     * Determine whether filesystem sync is disabled.
     *
     * @return true if filesystem sync is disabled, otherwise false
     */
    public synchronized boolean isDisableSync() {
        return this.disableSync;
    }

    /**
     * Enable explicit logging of transaction conflicts.
     *
     * <p>
     * If enabled, when a transaction fails to due to conflicts, the conflicting key ranges are logged.
     *
     * <p>
     * Default is false.
     *
     * @param dumpConflicts true to disable data sync
     */
    public synchronized void setDumpConflicts(final boolean dumpConflicts) {
        this.dumpConflicts = dumpConflicts;
    }

    /**
     * Determine whether explicit logging of transaction conflicts is enabled.
     *
     * @return true if  explicit logging of transaction conflicts is enabled, otherwise false
     */
    public synchronized boolean isDumpConflicts() {
        return this.dumpConflicts;
    }

    /**
     * Configure the priority of internal service threads.
     *
     * <p>
     * Default is -1, which means do not change thread priority from its default.
     *
     * @param threadPriority internal service thread priority, or -1 to leave thread priority unchanged
     * @throws IllegalStateException if this instance is already started
     * @throws IllegalArgumentException if {@code threadPriority} is not -1 and not in the range
     *  {@link Thread#MIN_PRIORITY} to {@link Thread#MAX_PRIORITY}
     */
    public synchronized void setThreadPriority(final int threadPriority) {
        Preconditions.checkArgument(threadPriority == -1
          || (threadPriority >= Thread.MIN_PRIORITY && threadPriority <= Thread.MAX_PRIORITY), "invalid threadPriority");
        Preconditions.checkState(this.role == null, "already started");
        this.threadPriority = threadPriority;
    }

    /**
     * Get the configured internal service thread priority.
     *
     * @return internal service thread priority, or -1 if not configured
     */
    public synchronized int getThreadPriority() {
        return this.threadPriority;
    }

    /**
     * Configure whether to increase the log level for certain performance-related events (e.g., "info" instead of "debug").
     *
     * <p>
     * Performance-related events are events that affect performance and would be considered abnormal in a perfectly
     * functioning Raft network, e.g., having to retransmit an acknowledgement.
     *
     * <p>
     * Default false.
     *
     * @param performanceLogging true for higher level logging of performance-related events
     */
    public void setPerformanceLogging(final boolean performanceLogging) {
        this.performanceLogging = performanceLogging;
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
        return this.currentConfig != null ? new TreeMap<>(this.currentConfig) : new TreeMap<>();
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
        return this.log.getLastIndex() > 0;
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
        return this.log.getLastAppliedTerm();
    }

    /**
     * Get this instance's last applied log entry index.
     *
     * @return last applied index, or zero if not running
     */
    public synchronized long getLastAppliedIndex() {
        return this.log.getLastAppliedIndex();
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
        return this.role != null ? new ArrayList<>(this.log.getUnapplied()) : null;
    }

    /**
     * Get the estimated total memory used by unapplied log entries.
     *
     * @return unapplied log entry memory usage, or zero if this instance is not running
     */
    public synchronized long getUnappliedLogMemoryUsage() {
        return this.log.getUnappliedLogMemoryUsage();
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
        list.sort(RaftKVTransaction.SORT_BY_ID);
        return list;
    }

    /**
     * Get the timestamp of the most recently committed linearizable transaction.
     *
     * <p>
     * This value can be used to confirm that the cluster is healthy.
     *
     * @return time of the most recent successful commit of a linearizable transaction, or null if none
     */
    public synchronized Timestamp getLinearizableCommitTimestamp() {
        if (this.linearizableCommitTimestamp != null && !this.linearizableCommitTimestamp.hasOccurred())
            this.linearizableCommitTimestamp = null;                                // TODO: fix rollover better
        return this.linearizableCommitTimestamp;
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
        if (this.logger.isDebugEnabled())
            this.debug("starting {} in directory {}", this.getClass().getName(), this.logDir);

        // Start up local database
        boolean success = false;
        try {

            // Create/verify log directory
            if (!this.logDir.exists())
                Files.createDirectories(this.logDir.toPath());
            if (!this.logDir.isDirectory())
                throw new IOException(String.format("file \"%s\" is not a directory", this.logDir));

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

            // Start background I/O thread
            assert this.ioThread == null;
            final String ioThreadName = "Raft I/O [" + this.identity + "]";
            this.ioThread = new IOThread(this.logDir, ioThreadName);
            if (this.threadPriority != -1)
                this.ioThread.setPriority(this.threadPriority);
            this.ioThread.start();

            // Start up service executor thread
            assert this.serviceExecutor == null;
            final String serviceThreadName = "Raft KV [" + this.identity + "]";
            this.serviceExecutor = Executors.newSingleThreadScheduledExecutor(action -> {
                final Thread thread = new Thread(action);
                synchronized (this) {
                    if (this.threadPriority != -1)
                        thread.setPriority(this.threadPriority);
                }
                thread.setName(serviceThreadName);
                return thread;
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
            final long lastAppliedTerm = this.decodeLong(LAST_APPLIED_TERM_KEY, 0);
            final long lastAppliedIndex = this.decodeLong(LAST_APPLIED_INDEX_KEY, 0);
            final Map<String, String> lastAppliedConfig = this.decodeConfig(LAST_APPLIED_CONFIG_KEY);
            this.flipflop = this.decodeBoolean(FLIP_FLOP_KEY);

            // Reset protocol version info
            this.protocolVersionMap.clear();

            // If we crashed part way through a snapshot install, recover by discarding partial install
            if (this.discardFlipFloppedStateMachine() && this.logger.isDebugEnabled())
                this.debug("detected partially applied snapshot install, discarding");

            // Reload outstanding log entries from disk
            this.loadLog(lastAppliedTerm, lastAppliedIndex, lastAppliedConfig);

            // Initialize commit index and key watch index
            this.commitIndex = this.log.getLastAppliedIndex();
            this.keyWatchIndex = this.commitIndex;

            // Rebuild current configuration
            this.currentConfig = this.log.buildCurrentConfig();

            // Show recovered state
            if (this.logger.isDebugEnabled()) {
                this.debug("recovered Raft state:"
                  + "\n  clusterId={}"
                  + "\n  currentTerm={}"
                  + "\n  lastAppliedEntry={}"
                  + "\n  lastAppliedConfig={}"
                  + "\n  currentConfig={}"
                  + "\n  votedFor={}"
                  + "\n  log={}",
                  this.clusterId != 0 ? String.format("0x%08x", this.clusterId) : "none",
                  this.currentTerm,
                  this.log.getLastAppliedIndex() + "t" + this.log.getLastAppliedTerm(),
                  this.log.getLastAppliedConfig(),
                  this.currentConfig,
                  votedFor != null ? "\"" + votedFor + "\"" : "nobody",
                  this.log.getUnapplied());
            }

            // Validate recovered state
            if (this.isConfigured()) {
                Preconditions.checkArgument(this.clusterId != 0, "inconsistent raft state");
                Preconditions.checkArgument(this.currentTerm > 0, "inconsistent raft state");
                Preconditions.checkArgument(this.log.getLastTerm() > 0, "inconsistent raft state");
                Preconditions.checkArgument(this.log.getLastIndex() > 0, "inconsistent raft state");
                Preconditions.checkArgument(!this.currentConfig.isEmpty(), "inconsistent raft state");
            } else {
                Preconditions.checkArgument(this.log.getLastAppliedTerm() == 0, "inconsistent raft state");
                Preconditions.checkArgument(this.log.getLastAppliedIndex() == 0, "inconsistent raft state");
                Preconditions.checkArgument(this.log.getLastTerm() == 0, "inconsistent raft state");
                Preconditions.checkArgument(this.log.getLastIndex() == 0, "inconsistent raft state");
                Preconditions.checkArgument(this.currentConfig.isEmpty(), "inconsistent raft state");
                Preconditions.checkArgument(this.log.getNumTotal() == 0, "inconsistent raft state");
            }

            // Start as follower (with unknown leader)
            this.changeRole(new FollowerRole(this, null, null, votedFor));

            // Done
            this.info("successfully started {} in directory {}", this, this.logDir);
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
        final IOThread ioThreadToShutdown;
        synchronized (this) {

            // Sanity check
            assert this.checkState();
            if (this.role == null || this.shuttingDown)
                return;

            // Set shutting down flag
            this.info("starting shutdown of {}", this);
            this.shuttingDown = true;

            // Fail all remaining open transactions
            for (RaftKVTransaction tx : new ArrayList<>(this.openTransactions.values())) {
                switch (tx.getState()) {
                case EXECUTING:
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
            try {
                if (!TimedWait.wait(this, 5000, this.openTransactions::isEmpty))
                    this.warn("open transactions not cleaned up during shutdown");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Snapshot field while synchronized
            ioThreadToShutdown = this.ioThread;
        }

        // Shut down the service executor and wait for pending tasks to finish
        this.serviceExecutor.shutdownNow();
        try {
            this.serviceExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Shutdown I/O thread
        ioThreadToShutdown.shutdown();
        try {
            ioThreadToShutdown.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Final cleanup
        synchronized (this) {
            this.serviceExecutor = null;
            this.ioThread = null;
            this.cleanup();
        }

        // Done
        this.info("completed shutdown of {}", this);
    }

    /**
     * Get the exception most recently thrown by the internal service thread, if any.
     * This is used mainly during testing.
     *
     * @return most recent service exception, or null if none
     */
    public synchronized Throwable getLastInternalError() {
        return this.lastInternalError;
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
        if (this.ioThread != null) {
            this.ioThread.shutdown();
            try {
                this.ioThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            this.ioThread = null;
        }
        this.kv.stop();
        Util.closeIfPossible(this.logDirChannel);
        this.logDirChannel = null;
        this.log.reset(false);
        this.random = null;
        this.network.stop();
        this.currentTerm = 0;
        this.currentTermStartTime = 0;
        this.commitIndex = 0;
        this.keyWatchIndex = 0;
        this.clusterId = 0;
        this.currentConfig = null;
        this.protocolVersionMap.clear();
        if (this.keyWatchTracker != null) {
            this.keyWatchTracker.close();
            this.keyWatchTracker = null;
        }
        this.linearizableCommitTimestamp = null;
        this.transmitting.clear();
        this.pendingService.clear();
        this.shuttingDown = false;
    }

    /**
     * Initialize our in-memory state from the persistent state reloaded from disk.
     * This is invoked on initial startup.
     */
    private void loadLog(long lastAppliedTerm, long lastAppliedIndex, Map<String, String> config) throws IOException {

        // Sanity check
        assert Thread.holdsLock(this);

        // Scan for log entry files
        final ArrayList<LogEntry> entryList = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(this.logDir.toPath())) {
            for (Path path : files) {
                final File file = path.toFile();

                // Ignore sub-directories (typically owned by the underlying k/v store)
                if (file.isDirectory())
                    continue;

                // Is this a log entry file?
                final long[] parse = LogEntry.parseFileName(file.getName());
                if (parse != null) {
                    if (this.logger.isDebugEnabled())
                        this.debug("recovering log file {}", file.getName());
                    final LogEntry logEntry;
                    try {
                        logEntry = LogEntry.fromFile(file, parse[0] > lastAppliedIndex);
                    } catch (IOException e) {
                        this.logger.error("error reading log file " + file.getName() + "; ignoring this file!", e);
                        continue;
                    }
                    entryList.add(logEntry);
                    continue;
                }

                // Is this a leftover temporary file?
                if (TEMP_FILE_PATTERN.matcher(file.getName()).matches()) {
                    if (this.logger.isDebugEnabled())
                        this.debug("deleting leftover temporary file {}", file.getName());
                    this.deleteFile(file, "leftover temporary file");
                    continue;
                }

                // Unknown
                this.warn("ignoring unrecognized file {} in my log directory", file.getName());
            }
        }

        // Sort log entries by index
        entryList.sort(LogEntry.SORT_BY_INDEX);

        // Verify the terms are sensible (increasing only)
        for (int i = 1; i < entryList.size(); i++) {
            final LogEntry logEntry = entryList.get(i);
            final LogEntry prevEntry = entryList.get(i - 1);
            if (logEntry.getTerm() < prevEntry.getTerm()) {
                this.nukeLogFilesFromList(entryList, "terms out of order");
                break;
            }
        }

        // Discard all but the last contigous range
        if (!entryList.isEmpty()) {
            int i = entryList.size() - 1;
            while (i > 0 && entryList.get(i - 1).getIndex() == entryList.get(i).getIndex() - 1)
                i--;
            this.nukeLogFilesFromList(entryList.subList(0, i), "non-contiguous");
        }

        // If first entry's index is greater than the last applied index + 1, discard everything
        if (!entryList.isEmpty() && entryList.get(0).getIndex() > lastAppliedIndex + 1)
            this.nukeLogFilesFromList(entryList, "too-high index");

        // If last entry's index is less than the last applied index, discard everything
        if (!entryList.isEmpty() && entryList.get(entryList.size() - 1).getIndex() < lastAppliedIndex)
            this.nukeLogFilesFromList(entryList, "too-low index");

        // Check the log entry at lastAppliedIndex has term equal to lastAppliedTerm
        if (!entryList.isEmpty() && entryList.get(0).getIndex() <= lastAppliedIndex) {
            final int lastAppliedListIndex = (int)(lastAppliedIndex - entryList.get(0).getIndex());
            if (entryList.get(lastAppliedListIndex).getTerm() != lastAppliedTerm)
                this.nukeLogFilesFromList(entryList, "last applied term mismatch");
        }

        // Reset log
        this.log.reset(lastAppliedTerm, lastAppliedIndex, entryList, config, false);
        if (this.logger.isDebugEnabled()) {
            this.debug("recovered {} applied and {} unapplied log entries ({} total bytes)",
              this.log.getNumApplied(), this.log.getNumUnapplied(), this.log.getUnappliedLogMemoryUsage());
        }
    }

    private void nukeLogFilesFromList(List<LogEntry> entries, String problem) {
        for (LogEntry logEntry : entries) {
            this.warn("deleting log file {}: {}", logEntry.getFile().getName(), problem);
            this.deleteFile(logEntry.getFile(), problem + " log file");
        }
        entries.clear();
    }

// Key Watches

    synchronized ListenableFuture<Void> watchKey(RaftKVTransaction tx, ByteData key) {
        Preconditions.checkState(this.role != null, "not started");
        tx.verifyExecuting();
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

        // Look for options from the PermazenTransactionManager
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

        // Look for OPTION_HIGH_PRIORITY option
        boolean highPriority = false;
        final Object value = options.get(OPTION_HIGH_PRIORITY);
        if (value instanceof Boolean)
            highPriority = (Boolean)value;
        else if (value instanceof String)
            highPriority = Boolean.valueOf((String)value);

        // Configure consistency level
        return this.createTransaction(consistency != null ? consistency : Consistency.LINEARIZABLE, highPriority);
    }

    /**
     * Create a new transaction with the specified consistency.
     *
     * <p>
     * Equivalent to {@link #createTransaction(Consistency, boolean) createTransaction(consistency, false)}.
     *
     * @param consistency consistency level
     * @return newly created transaction
     * @throws IllegalArgumentException if {@code consistency} is null
     * @throws IllegalStateException if this instance is not {@linkplain #start started} or in the process of shutting down
     */
    public RaftKVTransaction createTransaction(Consistency consistency) {
        return this.createTransaction(consistency, false);
    }

    /**
     * Create a new transaction with the specified consistency and with optional high priority.
     *
     * @param consistency consistency level
     * @param highPriority true to make transaction {@linkplain RaftKVTransaction#setHighPriority high priority}
     * @return newly created transaction
     * @throws IllegalArgumentException if {@code consistency} is null
     * @throws IllegalStateException if this instance is not {@linkplain #start started} or in the process of shutting down
     */
    public synchronized RaftKVTransaction createTransaction(Consistency consistency, boolean highPriority) {

        // Sanity check
        assert this.checkState();
        Preconditions.checkState(consistency != null, "null consistency");
        Preconditions.checkState(this.role != null, "not started");
        Preconditions.checkState(!this.shuttingDown, "shutting down");

        // Base transaction on the most recent log entry (unless it's supposed to be based on the most recent COMMITTED log entry).
        // This is itself a form of optimistic locking: we assume that the most recent log entry has a high probability of being
        // committed (in the Raft sense), which is of course required in order to commit any transaction based on it. But note
        // all reads will have to go through every unapplied log entry (as they would in any fully rebased transaction).
        final long maxIndex = consistency.isBasedOnCommittedLogEntry() ? this.commitIndex : this.log.getLastIndex();
        final MostRecentView view = new MostRecentView(this, maxIndex);
        final long baseTerm = view.getTerm();
        final long baseIndex = view.getIndex();

        // Create transaction
        final RaftKVTransaction tx = new RaftKVTransaction(this,
          consistency, baseTerm, baseIndex, view.getSnapshot(), view.getView());
        tx.setTimeout(this.commitTimeout);
        this.openTransactions.put(tx.txId, tx);

        // Set commit term+index if already known
        switch (consistency) {
        case UNCOMMITTED:
            tx.setCommittable();
            break;
        case EVENTUAL_COMMITTED:
            tx.setCommitInfo(baseTerm, baseIndex, null);
            tx.setCommittable();
            break;
        case EVENTUAL:
            tx.setCommitInfo(baseTerm, baseIndex, null);
            this.role.checkCommittable(tx);
            break;
        case LINEARIZABLE:
            break;
        default:
            assert false;
            break;
        }

        // Set high priority
        if (highPriority)
            tx.setHighPriority(true);

        // Done
        if (this.logger.isDebugEnabled())
            this.debug("created new transaction {}", tx);
        return tx;
    }

    /**
     * Make a transaction the high priority transaction on this node.
     */
    synchronized boolean setHighPriority(RaftKVTransaction tx, boolean highPriority) {

        // Demoting? That's easy.
        if (!highPriority) {
            if (tx == this.highPrioTx)
                this.highPrioTx = null;
            return true;
        }

        // Check transaction consistency
        if (!tx.getConsistency().equals(Consistency.LINEARIZABLE))
            return false;

        // Check transaction state
        switch (tx.getState()) {
        case EXECUTING:
        case COMMIT_READY:
        case COMMIT_WAITING:
            break;
        default:
            return false;
        }

        // Make transaction special
        if (this.logger.isDebugEnabled()) {
            if (this.highPrioTx != null)
                this.debug("setting high priority transaction {} (replacing {})", tx, this.highPrioTx);
            else
                this.debug("setting new high priority transaction {}", tx);
        }
        this.highPrioTx = tx;

        // Try to become leader if we're currently a follower and some other leader is established
        if (this.role instanceof FollowerRole) {
            final FollowerRole followerRole = (FollowerRole)this.role;
            if (followerRole.getLeaderIdentity() != null
              && followerRole.electionTimer.isRunning()
              && !followerRole.electionTimer.getDeadline().hasOccurred()) {
                if (this.logger.isDebugEnabled())
                    this.debug("starting early election on behalf of high priority transaction {}", tx);
                followerRole.startElection();
            }
        }

        // Done
        return true;
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
                if (this.role == null)
                    throw new StaleKVTransactionException(tx, "database is shutdown");

                // Check tx state
                switch (tx.getState()) {
                case EXECUTING:

                    // Transition to COMMIT_READY state
                    if (this.logger.isDebugEnabled())
                        this.debug("committing transaction {}", tx);
                    tx.setState(TxState.COMMIT_READY);
                    this.requestService(new CheckReadyTransactionService(this.role, tx));

                    // From this point on, throw a StaleKVTransactionException if accessed, instead of retry exception or whatever
                    tx.setFailure(null);

                    // Setup commit timer
                    if (tx.getTimeout() != 0) {
                        final Timer commitTimer = new Timer(this, "commit timer for " + tx,
                          new Service("commit timeout for tx#" + tx.txId, () -> {
                            switch (tx.getState()) {
                            case COMMIT_READY:
                            case COMMIT_WAITING:
                                this.fail(tx, new RetryKVTransactionException(tx, String.format(
                                  "transaction failed to complete within %dms (in state %s)", tx.getTimeout(), tx.getState())));
                                break;
                            default:
                                break;
                            }
                        }));
                        commitTimer.timeoutAfter(tx.getTimeout());
                        tx.setCommitTimer(commitTimer);
                    }
                    break;
                case CLOSED:                                        // this transaction has already been committed or rolled back
                    try {
                        tx.verifyExecuting();                       // always throws some kind of exception
                    } finally {
                        tx.setFailure(null);                        // from now on, throw StaleKVTransactionException if accessed
                    }
                    assert false;
                    return;
                default:                                            // another thread is already doing the commit
                    this.warn("simultaneous commit()'s requested for {} by two different threads", tx);
                    break;
                }
            }

            // Wait for completion
            try {
                tx.getCommitFuture().get();
            } catch (InterruptedException e) {
                throw new KVTransactionException(tx, "thread interrupted while waiting for commit", e);
            } catch (ExecutionException e) {
                final Throwable cause = e.getCause();
                ThrowableUtil.prependCurrentStackTrace(cause);
                Throwables.throwIfUnchecked(cause);
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
        if (this.role == null)
            return;

        // From this point on, throw a StaleKVTransactionException if accessed, instead of retry exception or whatever
        tx.setFailure(null);

        // Check tx state
        switch (tx.getState()) {
        case EXECUTING:
            if (this.logger.isDebugEnabled())
                this.debug("rolling back transaction {}", tx);
            this.cleanupTransaction(tx);
            break;
        case CLOSED:
            break;
        default:                                            // another thread is currently committing!
            this.warn("simultaneous commit() and rollback() requested for {} by two different threads", tx);
            break;
        }
    }

    // Clean up transaction and transition to state CLOSED. It's OK to invoke this more than once.
    synchronized void cleanupTransaction(RaftKVTransaction tx) {

        // Debug
        if (this.logger.isTraceEnabled())
            this.trace("cleaning up transaction {}", tx);

        // Do any per-role cleanups
        if (this.role != null)
            this.role.cleanupForTransaction(tx);

        // Cancel commit timer
        if (tx.getCommitTimer() != null)
            tx.getCommitTimer().cancel();

        // Remove from open transactions set
        this.openTransactions.remove(tx.txId);

        // Transition to CLOSED
        tx.setState(TxState.CLOSED);
        tx.setNoLongerRebasable();

        // Notify waiting thread if doing shutdown
        if (this.shuttingDown)
            this.notify();
    }

    // Mark a transaction as having succeeded; it must be in COMMIT_READY or COMMIT_WAITING
    void succeed(RaftKVTransaction tx) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert this.role != null;
        assert tx.getState().equals(TxState.COMMIT_READY) || tx.getState().equals(TxState.COMMIT_WAITING);

        // Succeed transaction
        if (this.logger.isDebugEnabled())
            this.debug("successfully committed {}", tx);
        tx.getCommitFuture().set(null);
        tx.setState(TxState.COMPLETED);
        tx.setNoLongerRebasable();
        if (tx.getConsistency().isGuaranteesUpToDateReads())
            this.linearizableCommitTimestamp = new Timestamp();
        this.role.cleanupForTransaction(tx);
    }

    // Mark a transaction as having failed
    void fail(RaftKVTransaction tx, KVTransactionException e) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert this.role != null;
        assert e != null;

        // Fail transaction
        if (this.logger.isDebugEnabled())
            this.debug("failing transaction {}: {}", tx, e.toString());
        switch (tx.getState()) {
        case EXECUTING:
            assert tx.getFailure() == null;
            tx.setFailure(e);
            this.cleanupTransaction(tx);
            break;
        case COMMIT_READY:
        case COMMIT_WAITING:
            tx.getCommitFuture().setException(e);
            tx.setState(TxState.COMPLETED);
            tx.setNoLongerRebasable();
            this.role.cleanupForTransaction(tx);
            break;
        default:                                        // too late, nobody cares
            return;
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
    void requestService(Service service) {
        assert Thread.holdsLock(this);
        assert service != null;
        if (!this.pendingService.add(service) || this.performingService)
            return;
        try {
            this.serviceExecutor.submit(() -> {
                try {
                    this.handlePendingService();
                } catch (Throwable t) {
                    RaftKVDatabase.this.error("exception in handlePendingService()", t);
                    this.lastInternalError = t;
                }
            });
        } catch (RejectedExecutionException e) {
            if (!this.shuttingDown) {
                this.warn("service executor task rejected, skipping", e);
                this.lastInternalError = e;
            }
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
                if (this.logger.isTraceEnabled())
                    this.trace("SERVICE [{}] in {}", service, this.role);
                try {
                    service.run();
                } catch (Throwable t) {
                    RaftKVDatabase.this.error("exception in {}", service, t);
                    this.lastInternalError = t;
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
        final ByteData dirtyPrefix = this.getFlipFloppedStateMachinePrefix();
        final boolean dirty;
        try (CloseableIterator<KVPair> i = this.kv.getRange(KeyRange.forPrefix(dirtyPrefix))) {
            dirty = i.hasNext();
        }
        if (dirty)
            this.kv.removeRange(dirtyPrefix, ByteUtil.getKeyAfterPrefix(dirtyPrefix));
        return dirty;
    }

    /**
     * Perform a state machine flip-flop operation. Normally this would happen after a successful snapshot install.
     *
     * @return true if successful, false if persistent store mutation failed
     */
    boolean flipFlopStateMachine(long term, long index, Map<String, String> config) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert term >= 0;
        assert index >= 0;
        if (this.logger.isDebugEnabled())
            this.debug("performing state machine flip-flop to {}t{} with config {}", index, term, config);
        if (config == null)
            config = new HashMap<>(0);

        // Prepare updates
        final Writes writes = new Writes();
        writes.getPuts().put(LAST_APPLIED_TERM_KEY, LongEncoder.encode(term));
        writes.getPuts().put(LAST_APPLIED_INDEX_KEY, LongEncoder.encode(index));
        writes.getPuts().put(LAST_APPLIED_CONFIG_KEY, this.encodeConfig(config));
        writes.getPuts().put(FLIP_FLOP_KEY, this.encodeBoolean(!this.flipflop));

        // Update persistent store
        try {
            this.kv.apply(writes, true);
        } catch (Exception e) {
            this.error("flip-flop error updating key/value store term/index to {}t{}", index, term, e);
            return false;
        }

        // Reset log, and delete any associated log files
        this.log.reset(term, index, config, true);

        // Update in-memory copy of persistent state
        this.flipflop = !this.flipflop;
        this.commitIndex = this.log.getLastAppliedIndex();
        final TreeMap<String, String> previousConfig = new TreeMap<>(this.currentConfig);
        this.currentConfig = this.log.buildCurrentConfig();
        if (!this.currentConfig.equals(previousConfig))
            this.info("apply new cluster configuration after snapshot install: {}", this.currentConfig);

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
        if (this.logger.isDebugEnabled())
            this.debug("advancing current term from {} -> {}", this.currentTerm, newTerm);

        // Update persistent store
        final Writes writes = new Writes();
        writes.getPuts().put(CURRENT_TERM_KEY, LongEncoder.encode(newTerm));
        writes.getRemoves().add(new KeyRange(VOTED_FOR_KEY));
        try {
            this.kv.apply(writes, true);
        } catch (Exception e) {
            this.error("error persisting new term {}", newTerm, e);
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
        this.info("joining cluster with ID {}", String.format("0x%08x", newClusterId));
        final Writes writes = new Writes();
        writes.getPuts().put(CLUSTER_ID_KEY, LongEncoder.encode(newClusterId));
        try {
            this.kv.apply(writes, true);
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
    ByteData getStateMachinePrefix() {
        return this.getStateMachinePrefix(false);
    }

    /**
     * Get the prefix for the flip-flopped state machine.
     */
    ByteData getFlipFloppedStateMachinePrefix() {
        return this.getStateMachinePrefix(true);
    }

    private ByteData getStateMachinePrefix(boolean flipFlopped) {
        return STATE_MACHINE_PREFIXES[(flipFlopped ^ this.flipflop) ? 1 : 0];
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
            this.pendingService.removeIf(service -> service.getRole() != null);
        }

        // Setup new role
        this.role = role;
        this.role.setup();
        if (this.logger.isDebugEnabled())
            this.debug("changing role to {}", role);

        // Check state
        assert this.checkState();
    }

    /**
     * Append a log entry to the Raft log.
     *
     * @param term new log entry term
     * @param newLogEntry entry to add; the {@linkplain NewLogEntry#getTempFile temporary file}
     *  must be already durably persisted, and it will be renamed
     * @return new {@link LogEntry}
     * @throws IOException if an error occurs
     */
    LogEntry appendLogEntry(long term, NewLogEntry newLogEntry) throws IOException {

        // Sanity check
        assert Thread.holdsLock(this);
        assert this.role != null;
        assert newLogEntry != null;

        // Get file length
        final LogEntry.Data data = newLogEntry.getData();
        final File tempFile = newLogEntry.getTempFile();
        final long fileLength = Util.getLength(tempFile);

        // Create new log entry
        final LogEntry logEntry = new LogEntry(term, this.log.getLastIndex() + 1, this.logDir, data, fileLength);
        if (this.logger.isDebugEnabled())
            this.debug("adding new log entry {} using {}", logEntry, tempFile.getName());

        // Atomically rename file and fsync() directory to durably persist
        Files.move(tempFile.toPath(), logEntry.getFile().toPath(), StandardCopyOption.ATOMIC_MOVE);
        if (this.logDirChannel != null && !this.disableSync)
            this.logDirChannel.force(true);

        // Temp file no longer exists, so don't try to delete it later
        newLogEntry.resetTempFile();

        // Add new log entry to in-memory log
        this.log.addLogEntry(logEntry);

        // Update current config
        if (logEntry.applyConfigChange(this.currentConfig))
            this.info("applying new cluster configuration from log entry {}: {}", logEntry, this.currentConfig);

        // Done
        return logEntry;
    }

// Object

    @Override
    public synchronized String toString() {
        return this.getClass().getSimpleName()
          + "[identity=" + (this.identity != null ? "\"" + this.identity + "\"" : null)
          + ",logDir=" + this.logDir
          + ",term=" + this.currentTerm
          + ",commitIndex=" + this.commitIndex
          + ",lastApplied=" + this.log.getLastAppliedIndex() + "t" + this.log.getLastAppliedTerm()
          + ",log=" + this.log.getUnapplied()
          + ",role=" + this.role
          + (this.shuttingDown ? ",shuttingDown" : "")
          + "]";
    }

// Network.Handler and Messaging

    private void handle(String sender, ByteBuffer buf) {

        // Decode message
        final int protocolVersion;
        final Message msg;
        try {
            protocolVersion = Message.decodeProtocolVersion(buf);
            msg = Message.decode(buf, protocolVersion);
        } catch (IllegalArgumentException e) {
            this.error("rec'd bogus message from {}, ignoring", sender, e);
            return;
        }

        // If message contains serialized mutation data, at some point we are going to need to write that data to a log entry file.
        // Instead of doing that (slow) operation while holding the lock, do it now, before we acquire the lock.
        ByteBuffer mutationData =
          msg instanceof AppendRequest ? ((AppendRequest)msg).getMutationData() :
          msg instanceof CommitRequest ? ((CommitRequest)msg).getMutationData() : null;
        final NewLogEntry newLogEntry;
        if (mutationData != null) {
            File tempFile = null;
            try {

                // Write serialized mutation data into temporary file
                tempFile = this.getTempFile();
                try (FileWriter output = new FileWriter(tempFile, this.disableSync)) {
                    final FileChannel channel = output.getFileOutputStream().getChannel();
                    for (ByteBuffer writeBuf = mutationData.asReadOnlyBuffer(); writeBuf.hasRemaining(); )
                        channel.write(writeBuf);
                }

                // Deserialize mutation data and create new log entry instance
                try (ByteBufferInputStream input = new ByteBufferInputStream(mutationData)) {
                    newLogEntry = new NewLogEntry(LogEntry.readData(input, true), tempFile);
                }

                // Indicate success
                tempFile = null;
            } catch (IOException e) {
                this.error("error persisting mutations from {}, ignoring", msg, e);
                return;
            } finally {
                if (tempFile != null)
                    this.deleteFile(tempFile, "new log entry temp file");
            }
        } else
            newLogEntry = null;

        // Handle message
        try {
            this.receiveMessage(sender, msg, protocolVersion, newLogEntry);
        } finally {
            if (newLogEntry != null)
                newLogEntry.cleanup(this);
        }
    }

    private synchronized void outputQueueEmpty(String address) {

        // Sanity check
        assert this.checkState();

        // Update transmitting status
        if (!this.transmitting.remove(address))
            return;
        if (this.logger.isTraceEnabled())
            this.trace("QUEUE_EMPTY address {} in {}", address, this.role);

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
            this.warn("can't send {} to unknown peer \"{}\"", msg, peer);
            return false;
        }

        // Determine protocol version to use
        final int protocolVersion = this.protocolVersionMap.getOrDefault(peer, Message.getCurrentProtocolVersion());

        // Encode messagse
        if (this.logger.isTraceEnabled())
            this.trace("XMIT {} to {} (protocol version {})", msg, address, protocolVersion);
        final ByteBuffer encodedMessage;
        try {
            encodedMessage = msg.encode(protocolVersion);
        } catch (IllegalArgumentException e) {                                      // can happen if peer running older code
            this.warn("can't send {} to peer \"{}\": {}", msg, peer, e.toString());
            return false;
        }

        // Send message
        if (this.network.send(address, encodedMessage)) {
            this.transmitting.add(address);
            return true;
        }

        // Transmit failed
        this.warn("transmit of {} to \"{}\" failed locally", msg, peer);
        return false;
    }

    synchronized void receiveMessage(String address, Message msg, int protocolVersion, final NewLogEntry newLogEntry) {

        // Sanity check newLogEntry
        assert newLogEntry == null || (msg instanceof AppendRequest || msg instanceof CommitRequest);

        // Sanity check
        assert Thread.holdsLock(this);
        assert this.checkState();
        if (this.role == null) {
            if (this.logger.isDebugEnabled())
                this.debug("rec'd {} rec'd in shutdown state; ignoring", msg);
            return;
        }

        // Debug
        if (this.logger.isTraceEnabled())
            this.trace("RECV {} in {} from {} (protocol version {})", msg, this.role, address, protocolVersion);

        // Sanity check cluster ID
        if (msg.getClusterId() == 0) {
            this.warn("rec'd {} with zero cluster ID from {}; ignoring", msg, address);
            return;
        }
        if (this.clusterId != 0 && msg.getClusterId() != this.clusterId) {
            this.warn("rec'd {} with foreign cluster ID {} {}; ignoring",
              msg, String.format("0x%08x", msg.getClusterId()), String.format("0x%08x", this.clusterId));
            return;
        }

        // Sanity check sender
        final String peer = msg.getSenderId();
        if (peer.equals(this.identity)) {
            this.warn("rec'd {} from myself (\"{}\", address {}); ignoring", msg, peer, address);
            return;
        }

        // Sanity check recipient
        final String dest = msg.getRecipientId();
        if (!dest.equals(this.identity)) {
            this.warn("rec'd misdirected {} intended for \"{}\" from {}; ignoring", msg, dest, address);
            return;
        }

        // Update sender's protocol version
        if (protocolVersion != -1) {
            final Integer previousVersion = this.protocolVersionMap.put(peer, protocolVersion);
            if (!((Integer)protocolVersion).equals(previousVersion) && this.logger.isDebugEnabled())
                this.debug("set protocol encoding version for peer \"{}\" to {}", peer, protocolVersion);
        }

        // Is my term too low? If so update and revert to follower
        if (msg.getTerm() > this.currentTerm) {

            // First check with current role; in some special cases we ignore this
            if (!this.role.mayAdvanceCurrentTerm(msg)) {
                if (this.logger.isTraceEnabled()) {
                    this.trace("rec'd {} with term {} > {} from \"{}\" but current role says to ignore it",
                      msg, msg.getTerm(), this.currentTerm, peer);
                }
                return;
            }

            // Revert to follower
            if (this.logger.isDebugEnabled()) {
                this.debug("rec'd {} with term {} > {} from \"{}\", updating term and {} follower",
                  msg.getClass().getSimpleName(), msg.getTerm(), this.currentTerm, peer,
                  this.role instanceof FollowerRole ? "remaining a" : "reverting to");
            }
            if (!this.advanceTerm(msg.getTerm()))
                return;
            this.changeRole(msg.isLeaderMessage() ? new FollowerRole(this, peer, address) : new FollowerRole(this));
        }

        // Is sender's term too low? Ignore it (except ping request)
        if (msg.getTerm() < this.currentTerm && !(msg instanceof PingRequest)) {
            if (this.logger.isDebugEnabled()) {
                this.debug("rec'd {} with term {} < {} from \"{}\" at {}, ignoring",
                  msg, msg.getTerm(), this.currentTerm, peer, address);
            }
            return;
        }

        // Handle message
        this.returnAddress = address;
        try {
            msg.visit(new MessageSwitch() {
                @Override
                public void caseAppendRequest(AppendRequest msg) {
                    RaftKVDatabase.this.role.caseAppendRequest(msg, newLogEntry);
                }
                @Override
                public void caseAppendResponse(AppendResponse msg) {
                    RaftKVDatabase.this.role.caseAppendResponse(msg);
                }
                @Override
                public void caseCommitRequest(CommitRequest msg) {
                    RaftKVDatabase.this.role.caseCommitRequest(msg, newLogEntry);
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

// I/O Thread

    synchronized void deleteFile(File file, String description) {
        if (this.ioThread == null) {                                    // should never happen
            Util.delete(file, description);
            return;
        }
        this.ioThread.deleteFile(file, description);
    }

    synchronized File getTempFile() throws IOException {
        if (this.ioThread == null)
            throw new IOException("instance is shutdown");
        return this.ioThread.getTempFile();
    }

    private static final class IOThread extends Thread {
        private static final int MAX_TEMP_FILES = 10;
        private static final int MAX_DELETE_FILES = 1000;

        private final Logger log = LoggerFactory.getLogger(this.getClass());
        private final File tempDir;
        private final ArrayBlockingQueue<FileInfo> availableTempFiles = new ArrayBlockingQueue<>(MAX_TEMP_FILES);
        private final ArrayBlockingQueue<FileInfo> filesToDelete = new ArrayBlockingQueue<>(MAX_DELETE_FILES);
        private final AtomicBoolean didWarnDelete = new AtomicBoolean();

        @GuardedBy("this")
        private boolean shutdown;

        private IOThread(File tempDir, String threadName) {
            super(threadName);
            Preconditions.checkArgument(tempDir != null);
            this.tempDir = tempDir;
        }

        public synchronized void shutdown() {
            this.shutdown = true;
            this.notifyAll();
        }

        public synchronized void deleteFile(File file, String description) {
            assert file != null;
            assert description != null;

            // Enqueue the task for the I/O thread, if able
            if (!this.shutdown) {
                try {
                    this.filesToDelete.add(new FileInfo(file, description));
                    this.notifyAll();
                } catch (IllegalStateException e) {
                    if (this.didWarnDelete.compareAndSet(false, true))
                        this.log.warn("file deletion queue is full (suppressing further warnings)");
                }
            }

            // We must do it ourselves
            Util.delete(file, description);
        }

        public synchronized File getTempFile() throws IOException {

            // Grab a free temp file from the ready queue, if any
            final FileInfo fileInfo;
            try {
                fileInfo = this.availableTempFiles.remove();
            } catch (NoSuchElementException e) {

                // We must do it ourselves
                return File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX, this.tempDir);
            }
            this.notifyAll();
            return fileInfo.getFile();
        }

        @Override
        public void run() {
            try {
                while (true) {

                    // Sleep until there's something to do
                    synchronized (this) {

                        // Wait for something to do
                        while (!this.shutdown && this.filesToDelete.isEmpty() && this.availableTempFiles.remainingCapacity() == 0) {
                            try {
                                this.wait();
                            } catch (InterruptedException e) {
                                this.log.warn(this + " interrupted, ignoring", e);
                            }
                        }

                        // Shutdown, if needed
                        if (this.shutdown)
                            break;
                    }

                    // Delete deletable files, if any
                    if (!this.filesToDelete.isEmpty())
                        this.deleteFiles(this.filesToDelete, true);

                    // Create a new temporary file, if needed
                    if (this.availableTempFiles.remainingCapacity() > 0) {
                        final File file;
                        try {
                            file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX, this.tempDir);
                        } catch (IOException e) {
                            this.log.error("error creating temporary file in {} ({} exiting)", this.tempDir, this, e);
                            break;      // exit thread - we can't reliabily create temp files
                        }
                        this.availableTempFiles.add(new FileInfo(file, "ready temporary file"));
                    }
                }
            } catch (ThreadDeath t) {
                throw t;
            } catch (Throwable t) {
                this.log.error("error in {}, bailing out", this, t);
            } finally {
                this.cleanup();
            }
        }

        private void cleanup() {
            this.deleteFiles(this.availableTempFiles, false);
            this.deleteFiles(this.filesToDelete, true);
        }

        private void deleteFiles(ArrayBlockingQueue<FileInfo> queue, boolean warn) {
            while (true) {

                // Get next file
                final FileInfo fileInfo;
                try {
                    fileInfo = queue.remove();
                } catch (NoSuchElementException e) {
                    break;
                }

                // Delete file
                Util.delete(fileInfo.getFile(), warn ? fileInfo.getDescription() : null);
            }
        }
    }

    private static final class FileInfo {

        private final File file;
        private final String description;

        FileInfo(File file, String description) {
            Preconditions.checkArgument(file != null);
            this.file = file;
            this.description = description;
        }

        public File getFile() {
            return this.file;
        }

        public String getDescription() {
            return this.description;
        }
    }

// Utility methods

    ByteData encodeBoolean(boolean value) {
        return ByteData.of(value ? 1 : 0);
    }

    boolean decodeBoolean(ByteData key) {
        final ByteData value = this.kv.get(key);
        return value != null && !value.isEmpty() && value.byteAt(0) != 0;
    }

    long decodeLong(ByteData key, long defaultValue) throws IOException {
        final ByteData value = this.kv.get(key);
        if (value == null)
            return defaultValue;
        try {
            return LongEncoder.decode(value);
        } catch (IllegalArgumentException e) {
            throw new IOException(String.format(
              "can't interpret encoded %s value %s under key %s", "long", ByteUtil.toString(value), ByteUtil.toString(key)), e);
        }
    }

    ByteData encodeString(String value) {
        return this.encodeData(output -> output.writeUTF(value));
    }

    String decodeString(ByteData key, String defaultValue) throws IOException {
        final ByteData value = this.kv.get(key);
        return value != null ? this.decodeData(value, "string", input -> input.readUTF()) : defaultValue;
    }

    Map<String, String> decodeConfig(ByteData key) throws IOException {
        final Map<String, String> config = new HashMap<>();
        final ByteData value = this.kv.get(key);
        if (value == null)
            return config;
        return this.decodeData(value, "config", input -> {
            while (true) {
                input.mark(1);
                if (input.read() == -1)
                    return config;
                input.reset();
                config.put(input.readUTF(), input.readUTF());
            }
        });
    }

    ByteData encodeConfig(Map<String, String> config) {
        return this.encodeData(output -> {
            for (Map.Entry<String, String> entry : config.entrySet()) {
                output.writeUTF(entry.getKey());
                output.writeUTF(entry.getValue());
            }
        });
    }

    private ByteData encodeData(Encoder encoder) {
        final ByteData.Writer buf = ByteData.newWriter();
        try (DataOutputStream output = new DataOutputStream(buf)) {
            encoder.encodeTo(output);
        } catch (IOException e) {
            throw new RuntimeException("unexpected error", e);
        }
        return buf.toByteData();
    }

    private <T> T decodeData(ByteData data, String desc, Decoder<T> decoder) throws IOException {
        try (DataInputStream input = new DataInputStream(data.newReader())) {
            return decoder.decodeFrom(input);
        } catch (IOException e) {
            throw new IOException(String.format("can't interpret encoded %s value %s", desc, ByteUtil.toString(data)), e);
        }
    }

    @FunctionalInterface
    private interface Encoder {
        void encodeTo(DataOutputStream output) throws IOException;
    }

    @FunctionalInterface
    private interface Decoder<T> {
        T decodeFrom(DataInputStream input) throws IOException;
    }

// Logging

    void trace(String msg, Object... args) {
        this.logger.trace(this.prefixLogFormat(msg), args);
    }

    void debug(String msg, Object... args) {
        this.logger.debug(this.prefixLogFormat(msg), args);
    }

    void info(String msg, Object... args) {
        this.logger.info(this.prefixLogFormat(msg), args);
    }

    void warn(String msg, Object... args) {
        this.logger.warn(this.prefixLogFormat(msg), args);
    }

    void error(String msg, Object... args) {
        this.logger.error(this.prefixLogFormat(msg), args);
    }

    void perfLog(String msg, Object... args) {
        if (this.performanceLogging)
            this.info("PERF: " + msg, args);
        else
            this.debug(msg, args);
    }

    boolean isPerfLogEnabled() {
        return this.performanceLogging ? this.logger.isInfoEnabled() : this.logger.isDebugEnabled();
    }

    private String prefixLogFormat(String format) {
        return new Timestamp() + " " + this.identity + ": " + format;
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

        // Check log state
        assert this.log.checkState();

        // Handle stopped state
        if (this.role == null) {
            assert this.log.getFirstIndex() == 0;
            assert this.random == null;
            assert this.currentTerm == 0;
            assert this.currentTermStartTime == 0;
            assert this.commitIndex == 0;
            assert this.currentConfig == null;
            assert this.clusterId == 0;
            assert this.logDirChannel == null;
            assert this.serviceExecutor == null;
            assert this.keyWatchTracker == null;
            assert this.linearizableCommitTimestamp == null;
            assert this.transmitting.isEmpty();
            assert this.openTransactions.isEmpty();
            assert this.highPrioTx == null;
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
        assert this.currentConfig != null;

        assert this.currentTerm >= this.log.getLastAppliedTerm();
        assert this.commitIndex >= this.log.getLastAppliedIndex();
        assert this.commitIndex <= this.log.getLastAppliedIndex() + this.log.getNumUnapplied();
        assert this.keyWatchIndex <= this.commitIndex;

        // Check configured vs. unconfigured
        if (this.isConfigured()) {
            assert this.clusterId != 0;
            assert this.currentTerm > 0;
            assert !this.currentConfig.isEmpty();
            assert this.currentConfig.equals(this.log.buildCurrentConfig());
        } else
            assert this.currentConfig.isEmpty();

        // Check role
        assert this.role.checkState();

        // Check transactions
        assert this.highPrioTx == null || this.openTransactions.containsKey(this.highPrioTx.txId);
        assert this.highPrioTx == null || this.highPrioTx.getState().compareTo(TxState.COMPLETED) < 0;
        for (RaftKVTransaction tx : this.openTransactions.values()) {
            try {
                assert !tx.getState().equals(TxState.CLOSED);
                tx.checkStateOpen(this.currentTerm, this.log.getLastIndex(), this.commitIndex);
                this.role.checkTransaction(tx);
            } catch (AssertionError e) {
                throw new AssertionError("checkState() failure for " + tx, e);
            }
        }
    }

    private boolean isWindows() {
        return System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH).contains("win");
    }
}
