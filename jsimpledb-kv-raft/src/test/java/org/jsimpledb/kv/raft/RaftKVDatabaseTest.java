
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.Callable;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.array.AtomicArrayKVStore;
import org.jsimpledb.kv.leveldb.LevelDBAtomicKVStore;
import org.jsimpledb.kv.test.KVDatabaseTest;
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
    public void setTestRaftDirPrefix(@Optional String raftDirPrefix, @Optional("5") int numNodes,
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
            switch (this.random.nextInt(3)) {
            case 1:
            {
                this.log.info("using LevelDB as key/value store for Raft test");
                final LevelDBAtomicKVStore levelkv = new LevelDBAtomicKVStore();
                levelkv.setDirectory(kvdir);
                levelkv.setCreateIfMissing(true);
                this.rafts[i].setKVStore(levelkv);
                break;
            }
//            case 2:
//            {
//                this.log.info("using RocksDB as key/value store for Raft test");
//                final RocksDBAtomicKVStore rockskv = new RocksDBAtomicKVStore();
//                rockskv.setDirectory(kvdir);
//                this.rafts[i].setKVStore(rockskv);
//                break;
//            }
            default:
            {
                this.log.info("using Array as key/value store for Raft test");
                final AtomicArrayKVStore arraykv = new AtomicArrayKVStore();
                arraykv.setDirectory(kvdir);
                arraykv.setCompactMaxDelay(arrayCompactMaxDelay);
                arraykv.setCompactLowWater(arrayCompactLowWater);
                arraykv.setCompactHighWater(arrayCompactHighWater);
                this.rafts[i].setKVStore(arraykv);
                break;
            }
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
        }
        for (int i = 0; i < numNodes; i++)
            this.rafts[i].start();
        for (int i = 0; i < numNodes; i++) {
            final int targetIndex = (i < 2 ? 1 : i) % numNodes;
            final int addIndex = (i + 1) % numNodes;
            final String node = this.rafts[addIndex].getIdentity();
            this.log.debug("adding node \"" + node + "\" to test cluster");
            this.try3times(this.rafts[targetIndex], new Transactional<Void>() {
                @Override
                public Void transact(KVTransaction tx) {
                    ((RaftKVTransaction)tx).configChange(node, node);
                    return null;
                }
            });
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
    public void testConflictingTransactions(final KVDatabase store) throws Exception {
        this.disruptCluster(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                RaftKVDatabaseTest.super.testConflictingTransactions(store);
                return null;
            }
        });
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
    public void testParallelTransactions(final KVDatabase store) throws Exception {
        this.disruptCluster(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                RaftKVDatabaseTest.super.testParallelTransactions(store);
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

    private <T> T disruptCluster(Callable<T> test) throws Exception {
        // TODO: randomly add/remove nodes
        return test.call();
    }

    @AfterClass
    @Override
    public void teardown() throws Exception {
        super.teardown();
        if (this.rafts != null) {
            for (RaftKVDatabase raft : this.rafts)
                raft.stop();
            for (TestNetwork network : this.raftNetworks)
                network.stop();
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
        }
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.rafts != null ? this.rafts[0] : null;
    }
}

