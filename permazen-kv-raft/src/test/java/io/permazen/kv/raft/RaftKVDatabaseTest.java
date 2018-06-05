
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.array.AtomicArrayKVStore;
import io.permazen.kv.leveldb.LevelDBAtomicKVStore;
import io.permazen.kv.mvcc.AtomicKVDatabase;
import io.permazen.kv.rocksdb.RocksDBAtomicKVStore;
import io.permazen.kv.sqlite.SQLiteKVDatabase;
import io.permazen.kv.test.KVDatabaseTest;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.concurrent.Callable;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class RaftKVDatabaseTest extends KVDatabaseTest {

    private RaftKVDatabase[] rafts;
    private TestNetwork[] raftNetworks;
    private File topRaftDir;

    @BeforeClass(groups = "configure")
    @Parameters({
      "raftDirPrefix",
      "raftNumNodes",
      "raftKVStore",
      "raftCommitTimeout",
      "raftMinElectionTimeout",
      "raftMaxElectionTimeout",
      "raftHeartbeatTimeout",
      "raftMaxTransactionDuration",
      "raftFollowerProbingEnabled",
      "raftNetworkDelayMillis",
      "raftNetworkDropRatio",
      "arrayCompactMaxDelay",
      "arrayCompactSpaceLowWater",
      "arrayCompactSpaceHighWater",
    })
    public void setTestRaftDirPrefix(
      @Optional String raftDirPrefix, @Optional("5") int numNodes, @Optional final String kvstoreType,
      @Optional("2500") int commitTimeout, @Optional("300") int minElectionTimeout, @Optional("350") int maxElectionTimeout,
      @Optional("150") int heartbeatTimeout, @Optional("5000") int maxTransactionDuration,
      @Optional("true") boolean followerProbingEnabled,
      @Optional("25") int networkDelayMillis, @Optional("0.075") float networkDropRatio,
      @Optional("90") int arrayCompactMaxDelay,
      @Optional("65536") int arrayCompactLowWater,
      @Optional("1073741824") int arrayCompactHighWater)
      throws Exception {
        if (raftDirPrefix == null)
            return;
        this.raftNetworks = new TestNetwork[numNodes];
        this.rafts = new RaftKVDatabase[numNodes];
        this.topRaftDir = File.createTempFile(raftDirPrefix, null);
        Assert.assertTrue(this.topRaftDir.delete());
        Assert.assertTrue(this.topRaftDir.mkdirs());
        this.topRaftDir.deleteOnExit();
        for (int i = 0; i < numNodes; i++) {
            final String name = "node" + i;
            final File dir = new File(this.topRaftDir, name);
            dir.mkdirs();
            this.raftNetworks[i] = new TestNetwork(name, networkDelayMillis, networkDropRatio);
            this.rafts[i] = new RaftKVDatabase();
            final File kvdir = new File(dir, "kvstore");
            kvdir.mkdirs();
            String nodeKVStoreType = kvstoreType;
            if (nodeKVStoreType == null) {
                final String[] kvstoreTypes = new String[] { "leveldb", "rocksdb", /*"sqlite",*/ "array" };
                nodeKVStoreType = kvstoreTypes[this.random.nextInt(kvstoreTypes.length)];
            }
            this.log.info("using " + nodeKVStoreType + " as key/value store on " + name);
            switch (nodeKVStoreType) {
            case "leveldb":
            {
                final LevelDBAtomicKVStore levelkv = new LevelDBAtomicKVStore();
                levelkv.setDirectory(kvdir);
                levelkv.setCreateIfMissing(true);
                this.rafts[i].setKVStore(levelkv);
                break;
            }
            case "rocksdb":
            {
                final RocksDBAtomicKVStore rockskv = new RocksDBAtomicKVStore();
                rockskv.setDirectory(kvdir);
                this.rafts[i].setKVStore(rockskv);
                break;
            }
            case "sqlite":
            {
                final SQLiteKVDatabase sqlite = new SQLiteKVDatabase();
                sqlite.setDatabaseFile(new File(kvdir, "kvstore.sqlite3"));
                sqlite.setExclusiveLocking(true);
                sqlite.setPragmas(Arrays.asList("journal_mode=WAL"));
                final AtomicKVDatabase kvstore = new AtomicKVDatabase(sqlite);
                this.rafts[i].setKVStore(kvstore);
                break;
            }
            case "array":
            {
                final AtomicArrayKVStore arraykv = new AtomicArrayKVStore();
                arraykv.setDirectory(kvdir);
                arraykv.setCompactMaxDelay(arrayCompactMaxDelay);
                arraykv.setCompactLowWater(arrayCompactLowWater);
                arraykv.setCompactHighWater(arrayCompactHighWater);
                this.rafts[i].setKVStore(arraykv);
                break;
            }
            default:
                throw new IllegalArgumentException("unknown k/v store type `" + nodeKVStoreType + "'");
            }
            this.rafts[i].setLogDirectory(dir);
            this.rafts[i].setNetwork(this.raftNetworks[i]);
            this.rafts[i].setIdentity(name);
            this.rafts[i].setCommitTimeout(commitTimeout);
            this.rafts[i].setMinElectionTimeout(minElectionTimeout);
            this.rafts[i].setMaxElectionTimeout(maxElectionTimeout);
            this.rafts[i].setHeartbeatTimeout(heartbeatTimeout);
            this.rafts[i].setMaxTransactionDuration(maxTransactionDuration);
            this.rafts[i].setFollowerProbingEnabled(followerProbingEnabled);
            this.rafts[i].setDumpConflicts(true);
        }
        for (int i = 0; i < numNodes; i++)
            this.rafts[i].start();
        for (int i = 0; i < numNodes; i++) {
            final int targetIndex = (i < 2 ? 1 : i) % numNodes;
            final int addIndex = (i + 1) % numNodes;
            final String node = this.rafts[addIndex].getIdentity();
            this.log.debug("adding node \"" + node + "\" to test cluster");
            this.tryNtimes(this.rafts[targetIndex], tx -> ((RaftKVTransaction)tx).configChange(node, node));

            // Verify first node can commit
            if (i == 0) {
                this.tryNtimes(this.rafts[targetIndex], tx -> {
                    tx.getRange(null, null, false).hasNext();
                });
            }
        }
    }

    @Override
    public void testSimpleStuff(final KVDatabase store) throws Exception {
        this.disruptCluster(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                RaftKVDatabaseTest.super.testSimpleStuff(store);
                return null;
            }
        });
    }

    @Override
    public void testReadWriteConflict(final KVDatabase store) throws Exception {
        this.disruptCluster(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                RaftKVDatabaseTest.super.testReadWriteConflict(store);
                return null;
            }
        });
    }

    @Override
    public void testWriteSkewAnomaly(final KVDatabase store) throws Exception {
        this.disruptCluster(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                RaftKVDatabaseTest.super.testWriteSkewAnomaly(store);
                return null;
            }
        });
    }

    @Override
    protected boolean allowBothTransactionsToFail() {
        return true;
    }

    @Override
    protected int getNumTries() {
        return 20;
    }

    @Override
    public void testNonconflictingTransactions(final KVDatabase store) throws Exception {
        this.disruptCluster(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                RaftKVDatabaseTest.super.testNonconflictingTransactions(store);
                return null;
            }
        });
    }

    @Override
    public void testParallelTransactions(KVDatabase store) throws Exception {
        this.testParallelTransactions(this.rafts);
    }

    @Override
    public void testParallelTransactions(final KVDatabase[] stores) throws Exception {
        this.disruptCluster(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                RaftKVDatabaseTest.super.testParallelTransactions(stores);
                return null;
            }
        });
    }

    @Override
    public void testSequentialTransactions(final KVDatabase store) throws Exception {
        this.disruptCluster(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                RaftKVDatabaseTest.super.testSequentialTransactions(store);
                return null;
            }
        });
    }

    @Override
    public void testKeyWatch(final KVDatabase store) throws Exception {
        this.disruptCluster(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                RaftKVDatabaseTest.super.testKeyWatch(store);
                return null;
            }
        });
    }

    @Override
    protected RaftKVTransaction doCreateTransaction(KVDatabase kvdb) {
        return ((RaftKVDatabase)kvdb).createTransaction(Consistency.LINEARIZABLE, this.random.nextInt(20) == 0);
    }

    private <T> T disruptCluster(Callable<T> test) throws Exception {
        // TODO: randomly add/remove nodes
        return test.call();
    }

    @AfterClass
    public void teardownRafts() throws Exception {
        if (this.rafts != null) {

            // Shut them down
            for (RaftKVDatabase raft : this.rafts)
                raft.stop();
            for (TestNetwork network : this.raftNetworks)
                network.stop();

            // Check for undeleted temporary files
            for (RaftKVDatabase raft : this.rafts) {
                try (DirectoryStream<Path> files = Files.newDirectoryStream(raft.logDir.toPath())) {
                    for (Path path : files) {
                        final File file = path.toFile();
                        if (RaftKVDatabase.TEMP_FILE_PATTERN.matcher(file.getName()).matches())
                            throw new Exception("leftover temp file: " + file);
                    }
                }
            }

            // Clean up temporary files
            Files.walkFileTree(this.topRaftDir.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });

            // Check for exceptions
            for (RaftKVDatabase raft : this.rafts) {
                final Throwable t = raft.getLastInternalError();
                if (t != null)
                    throw new Exception("internal error in " + raft, t);
            }
        }
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.rafts != null ? this.rafts[0] : null;
    }
}

