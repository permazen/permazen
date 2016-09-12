
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.test;

import com.google.common.base.Converter;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.TransactionTimeoutException;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ConvertedNavigableMap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public abstract class KVDatabaseTest extends KVTestSupport {

    protected ExecutorService executor;

    @BeforeClass(dependsOnGroups = "configure")
    public void setup() throws Exception {
        this.executor = Executors.newFixedThreadPool(33);
        for (KVDatabase[] kvdb : this.getDBs()) {
            if (kvdb.length > 0)
                kvdb[0].start();
        }
    }

    @AfterClass
    public void teardown() throws Exception {
        this.executor.shutdown();
        for (KVDatabase[] kvdb : this.getDBs()) {
            if (kvdb.length > 0)
                kvdb[0].stop();
        }
    }

    @DataProvider(name = "kvdbs")
    protected KVDatabase[][] getDBs() {
        final KVDatabase kvdb = this.getKVDatabase();
        return kvdb != null ? new KVDatabase[][] { { kvdb } } : new KVDatabase[0][];
    }

    protected abstract KVDatabase getKVDatabase();

    @Test(dataProvider = "kvdbs")
    public void testSimpleStuff(KVDatabase store) throws Exception {

        // Debug
        this.log.info("starting testSimpleStuff() on " + store);

        // Clear database
        this.log.info("testSimpleStuff() on " + store + ": clearing database");
        this.try3times(store, new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                tx.removeRange(null, null);
                return null;
            }
        });
        this.log.info("testSimpleStuff() on " + store + ": done clearing database");

        // Verify database is empty
        this.log.info("testSimpleStuff() on " + store + ": verifying database is empty");
        this.try3times(store, new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                KVPair p = tx.getAtLeast(null);
                Assert.assertNull(p);
                p = tx.getAtMost(null);
                Assert.assertNull(p);
                Iterator<KVPair> it = tx.getRange(null, null, false);
                Assert.assertFalse(it.hasNext());
                return null;
            }
        });
        this.log.info("testSimpleStuff() on " + store + ": done verifying database is empty");

        // tx 1
        this.log.info("testSimpleStuff() on " + store + ": starting tx1");
        this.try3times(store, new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                final byte[] x = tx.get(b("01"));
                if (x != null)
                    Assert.assertEquals(tx.get(b("01")), b("02"));          // transaction was retried even though it succeeded
                tx.put(b("01"), b("02"));
                Assert.assertEquals(tx.get(b("01")), b("02"));
                return null;
            }
        });
        this.log.info("testSimpleStuff() on " + store + ": committed tx1");

        // tx 2
        this.log.info("testSimpleStuff() on " + store + ": starting tx2");
        this.try3times(store, new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                final byte[] x = tx.get(b("01"));
                Assert.assertNotNull(x);
                Assert.assertTrue(Arrays.equals(x, b("02")) || Arrays.equals(x, b("03")));
                tx.put(b("01"), b("03"));
                Assert.assertEquals(tx.get(b("01")), b("03"));
                return null;
            }
        });
        this.log.info("testSimpleStuff() on " + store + ": committed tx2");

        // tx 3
        this.log.info("testSimpleStuff() on " + store + ": starting tx3");
        this.try3times(store, new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                final byte[] x = tx.get(b("01"));
                Assert.assertEquals(x, b("03"));
                tx.put(b("10"), b("01"));
                return null;
            }
        });
        this.log.info("testSimpleStuff() on " + store + ": committed tx3");

        // Check stale access
        this.log.info("testSimpleStuff() on " + store + ": checking stale access");
        final KVTransaction tx = this.try3times(store, new Transactional<KVTransaction>() {
            @Override
            public KVTransaction transact(KVTransaction tx) {
                return tx;
            }
        });
        try {
            tx.get(b("01"));
            assert false;
        } catch (StaleTransactionException e) {
            // expected
        }
        this.log.info("finished testSimpleStuff() on " + store);
    }

    @Test(dataProvider = "kvdbs")
    public void testKeyWatch(KVDatabase store) throws Exception {

        // Debug
        this.log.info("starting testKeyWatch() on " + store);

        // Clear database
        this.log.info("testKeyWatch() on " + store + ": clearing database");
        this.try3times(store, new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                tx.removeRange(null, null);
                return null;
            }
        });
        this.log.info("testKeyWatch() on " + store + ": done clearing database");

        // Set up the modifications we want to test
        final ArrayList<Transactional<Void>> mods = new ArrayList<>();
        mods.add(new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                tx.put(b("0123"), b("4567"));
                return null;
            }
        });
        mods.add(new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                tx.put(b("0123"), b("89ab"));
                return null;
            }
        });
        mods.add(new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                tx.put(b("0123"), tx.encodeCounter(1234));
                return null;
            }
        });
        mods.add(new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                tx.adjustCounter(b("0123"), 99);
                return null;
            }
        });
        mods.add(new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                tx.removeRange(b("01"), b("02"));
                return null;
            }
        });
        mods.add(new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                tx.put(b("0123"), b(""));
                return null;
            }
        });
        mods.add(new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                tx.remove(b("0123"));
                return null;
            }
        });

        // Set watches, perform modifications, and test notifications
        for (Transactional<Void> mod : mods) {

            // Set watch
            this.log.info("testKeyWatch() on " + store + ": creating key watch for " + mod);
            final Future<Void> watch = this.try3times(store, new Transactional<Future<Void>>() {
                @Override
                public Future<Void> transact(KVTransaction tx) {
                    try {
                        return tx.watchKey(b("0123"));
                    } catch (UnsupportedOperationException e) {
                        return null;
                    }
                }
            });
            if (watch == null) {
                this.log.info("testKeyWatch() on " + store + ": key watches not supported, bailing out");
                return;
            }
            this.log.info("testKeyWatch() on " + store + ": created key watch: " + watch);

            // Perform modification
            this.log.info("testKeyWatch() on " + store + ": testing " + mod);
            this.try3times(store, mod);

            // Get notification
            this.log.info("testKeyWatch() on " + store + ": waiting for notification");
            final long start = System.nanoTime();
            watch.get(1, TimeUnit.SECONDS);
            this.log.info("testKeyWatch() on " + store + ": got notification in " + ((System.nanoTime() - start) / 1000000) + "ms");
        }

        // Done
        this.log.info("finished testKeyWatch() on " + store);
    }

    @Test(dataProvider = "kvdbs")
    public void testConflictingTransactions(KVDatabase store) throws Exception {

        // Clear database
        this.log.info("starting testConflictingTransactions() on " + store);
        this.try3times(store, new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                tx.removeRange(null, null);
                return null;
            }
        });

        // Both read the same key
        final KVTransaction[] txs = new KVTransaction[] { store.createTransaction(), store.createTransaction() };
        this.log.info("tx[0] is " + txs[0]);
        this.log.info("tx[1] is " + txs[1]);
        this.executor.submit(new Reader(txs[0], b("10"))).get();
        this.executor.submit(new Reader(txs[1], b("10"))).get();

        // Both write to the same key but with different values
        final String[] fails = new String[] { "uninitialized status", "uninitialized status" };
        Future<?>[] futures = new Future<?>[] {
          this.executor.submit(new Writer(txs[0], b("10"), b("01"))),
          this.executor.submit(new Writer(txs[1], b("10"), b("02")))
        };

        // See what happened - we might have gotten a conflict at write time
        for (int i = 0; i < 2; i++) {
            try {
                futures[i].get();
                this.log.info(txs[i] + " #" + (i + 1) + " succeeded on write");
                fails[i] = null;
            } catch (Exception e) {
                while (e instanceof ExecutionException)
                    e = (Exception)e.getCause();
                if (!(e instanceof RetryTransactionException))
                    throw new AssertionError("wrong exception type: " + e, e);
                final RetryTransactionException retry = (RetryTransactionException)e;
                Assert.assertSame(retry.getTransaction(), txs[i]);
                this.log.info(txs[i] + " #" + (i + 1) + " failed on write");
                if (this.log.isTraceEnabled())
                    this.log.trace(txs[i] + " #" + (i + 1) + " write failure exception trace:", e);
                fails[i] = "" + e;
            }
        }

        // Show contents of surviving transactions; note exception(s) could occur here also
        for (int i = 0; i < 2; i++) {
            if (fails[i] == null) {
                final Exception e = this.showKV(txs[i], "tx[" + i + "] of " + store + " after write");
                if (e != null)
                    fails[i] = "" + e;
            }
        }

        // If both succeeded, then we should get a conflict on commit instead
        for (int i = 0; i < 2; i++) {
            if (fails[i] == null)
                futures[i] = this.executor.submit(new Committer(txs[i]));
        }
        for (int i = 0; i < 2; i++) {
            if (fails[i] == null) {
                try {
                    futures[i].get();
                    this.log.info(txs[i] + " #" + (i + 1) + " succeeded on commit");
                    fails[i] = null;
                } catch (Exception e) {
                    while (e instanceof ExecutionException)
                        e = (Exception)e.getCause();
                    assert e instanceof RetryTransactionException : "wrong exception type: " + e;
                    final RetryTransactionException retry = (RetryTransactionException)e;
                    Assert.assertSame(retry.getTransaction(), txs[i]);
                    this.log.info(txs[i] + " #" + (i + 1) + " failed on commit");
                    if (this.log.isTraceEnabled())
                        this.log.trace(txs[i] + " #" + (i + 1) + " commit failure exception trace:", e);
                    fails[i] = "" + e;
                }
            }
        }

        // Exactly one should have failed and one should have succeeded (for most databases)
        if (!this.allowBothTransactionsToFail()) {
            assert fails[0] == null || fails[1] == null : "both transactions failed:"
              + "\n  fails[0]: " + fails[0] + "\n  fails[1]: " + fails[1];
        }
        assert fails[0] != null || fails[1] != null : "both transactions succeeded";
        this.log.info("exactly one transaction failed:\n  fails[0]: " + fails[0] + "\n  fails[1]: " + fails[1]);

        // Verify the resulting change is consistent with the tx that succeeded
        final byte[] expected = fails[0] == null ? b("01") : fails[1] == null ? b("02") : null;
        if (expected != null) {
            final KVTransaction tx2 = store.createTransaction();
            this.showKV(tx2, "TX2 of " + store);
            byte[] x = this.executor.submit(new Reader(tx2, b("10"))).get();
            Assert.assertEquals(x, expected);
            tx2.rollback();
        }
        this.log.info("finished testConflictingTransactions() on " + store);
    }

    protected boolean allowBothTransactionsToFail() {
        return false;
    }

    @Test(dataProvider = "kvdbs")
    public void testNonconflictingTransactions(KVDatabase store) throws Exception {

        // Clear database
        this.log.info("starting testNonconflictingTransactions() on " + store);
        this.try3times(store, new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                tx.removeRange(null, null);
                return null;
            }
        });

        // Multiple concurrent transactions with overlapping read ranges and non-intersecting write ranges
        int done = 0;
        KVTransaction[] txs = new KVTransaction[10];
        for (int i = 0; i < txs.length; i++)
            txs[i] = store.createTransaction();
        while (true) {
            boolean finished = true;
            for (int i = 0; i < txs.length; i++) {
                if (txs[i] == null)
                    continue;
                finished = false;
                Future<?> rf = this.executor.submit(new Reader(txs[i], new byte[] { (byte)i }, true));
                Future<?> wf = this.executor.submit(new Writer(txs[i], new byte[] { (byte)(i + 128) }, b("02")));
                for (Future<?> f : new Future<?>[] { rf, wf }) {
                    try {
                        f.get();
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof RetryTransactionException) {
                            txs[i] = store.createTransaction();
                            break;
                        }
                        throw e;
                    }
                }
            }
            if (finished)
                break;
            for (int i = 0; i < txs.length; i++) {
                if (txs[i] == null)
                    continue;
                try {
                    txs[i].commit();
                } catch (RetryTransactionException e) {
                    txs[i] = store.createTransaction();
                    continue;
                }
                txs[i] = null;
            }
        }
        this.log.info("finished testNonconflictingTransactions() on " + store);
    }

    /**
     * This test runs transactions in parallel and verifies there is no "leakage" between them.
     * Database must be configured for linearizable isolation.
     *
     * @param store underlying store
     * @throws Exception if an error occurs
     */
    @Test(dataProvider = "kvdbs")
    public void testParallelTransactions(KVDatabase store) throws Exception {
        this.log.info("starting testParallelTransactions() on " + store);
        for (int count = 0; count < 25; count++) {
            final RandomTask[] tasks = new RandomTask[25];
            for (int i = 0; i < tasks.length; i++) {
                tasks[i] = new RandomTask(i, store, this.random.nextLong());
                tasks[i].start();
            }
            for (int i = 0; i < tasks.length; i++)
                tasks[i].join();
            for (int i = 0; i < tasks.length; i++) {
                final Throwable fail = tasks[i].getFail();
                if (fail != null)
                    throw new Exception("task #" + i + " failed: >>>" + this.show(fail).trim() + "<<<");
            }
        }
        this.log.info("finished testParallelTransactions() on " + store);
        if (store instanceof Closeable)
            ((Closeable)store).close();
    }

    /**
     * This test runs transactions sequentially and verifies that each transaction sees
     * the changes that were committed in the previous transaction.
     *
     * @param store underlying store
     * @throws Exception if an error occurs
     */
    @Test(dataProvider = "kvdbs")
    public void testSequentialTransactions(KVDatabase store) throws Exception {
        this.log.info("starting testSequentialTransactions() on " + store);

        // Clear database
        this.try3times(store, new Transactional<Void>() {
            @Override
            public Void transact(KVTransaction tx) {
                tx.removeRange(null, null);
                return null;
            }
        });

        // Keep an in-memory record of what is in the committed database
        final TreeMap<byte[], byte[]> committedData = new TreeMap<byte[], byte[]>(ByteUtil.COMPARATOR);

        // Run transactions
        for (int i = 0; i < 50; i++) {
            final RandomTask task = new RandomTask(i, store, committedData, this.random.nextLong());
            task.run();
            final Throwable fail = task.getFail();
            if (fail != null)
                throw new Exception("task #" + i + " failed: >>>" + this.show(fail).trim() + "<<<");
        }
        this.log.info("finished testSequentialTransactions() on " + store);
    }

    protected <V> V try3times(KVDatabase kvdb, Transactional<V> transactional) {
        RetryTransactionException retry = null;
        for (int count = 0; count < 3; count++) {
            final KVTransaction tx = kvdb.createTransaction();
            try {
                final V result = transactional.transact(tx);
                tx.commit();
                return result;
            } catch (RetryTransactionException e) {
                KVDatabaseTest.this.log.debug("retry #" + (count + 1) + " on " + e);
                retry = e;
            }
            try {
                Thread.sleep(100 + count * 200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw retry;
    }

    protected interface Transactional<V> {
        V transact(KVTransaction kvt);
    }

// RandomTask

    public class RandomTask extends Thread {

        private final int id;
        private final KVDatabase store;
        private final Random random;
        private final TreeMap<byte[], byte[]> committedData;            // tracks actual committed data, if known
        private final NavigableMap<String, String> committedDataView;
        private final Converter<String, byte[]> converter = ByteUtil.STRING_CONVERTER.reverse();

        private Throwable fail;

        public RandomTask(int id, KVDatabase store, long seed) {
            this(id, store, null, seed);
        }

        public RandomTask(int id, KVDatabase store, TreeMap<byte[], byte[]> committedData, long seed) {
            super("Random[" + id + "]");
            this.id = id;
            this.store = store;
            this.committedData = committedData;
            this.committedDataView = this.stringView(this.committedData);
            this.random = new Random(seed);
            this.log("seed = " + seed);
        }

        @Override
        public void run() {
            try {
                this.test();
                this.log("succeeded");
            } catch (Throwable t) {
                final StringWriter buf = new StringWriter();
                t.printStackTrace(new PrintWriter(buf, true));
                this.log("failed: " + t + "\n" + buf.toString());
                this.fail = t;
            }
        }

        public Throwable getFail() {
            return this.fail;
        }

        @SuppressWarnings("unchecked")
        private void test() throws Exception {

            // Keep track of key/value pairs that we know should exist in the transaction
            final TreeMap<byte[], byte[]> knownValues = new TreeMap<byte[], byte[]>(ByteUtil.COMPARATOR);
            final NavigableMap<String, String> knownValuesView = this.stringView(knownValues);

            // Create transaction
            final KVTransaction tx = this.store.createTransaction();

            // Load actual committed database contents (if known) into "known values" tracker
            if (this.committedData != null)
                knownValues.putAll(this.committedData);

            // Save a copy of committed data
            final TreeMap<byte[], byte[]> previousCommittedData = this.committedData != null ?
              (TreeMap<byte[], byte[]>)this.committedData.clone() : null;
            //final NavigableMap<String, String> previousCommittedDataView = this.stringView(previousCommittedData);

            // Verify committed data is accurate before starting
            if (this.committedData != null)
                Assert.assertEquals(this.stringView(this.readDatabase(tx)), knownValuesView);

            // Note: if this.committedData != null, then knownValues will exactly track the transaction, otherwise,
            // knownValues only contains values we know are in there; nothing is known about uncontained values.

            // Make a bunch of random changes
            Boolean committed = null;
            try {
                final int limit = this.r(1000);
                for (int j = 0; j < limit; j++) {
                    byte[] key;
                    byte[] val;
                    byte[] min;
                    byte[] max;
                    KVPair pair;
                    int option = this.r(62);
                    boolean knownValuesChanged = false;
                    if (option < 10) {                                              // get
                        key = this.rb(2, false);
                        val = tx.get(key);
                        this.log("get: " + s(key) + " -> " + s(val));
                        if (val == null) {
                            Assert.assertTrue(!knownValues.containsKey(key),
                              this + ": get(" + s(key) + ") returned null but knownValues has " + knownValuesView);
                        } else if (knownValues.containsKey(key)) {
                            Assert.assertEquals(s(knownValues.get(key)), s(val),
                              this + ": get(" + s(key) + ") returned " + s(val) + " but knownValues has " + knownValuesView);
                        } else {
                            knownValues.put(key, val);
                            knownValuesChanged = true;
                        }
                    } else if (option < 20) {                                       // put
                        key = this.rb(2, false);
                        val = this.rb(2, true);
                        this.log("put: " + s(key) + " -> " + s(val));
                        tx.put(key, val);
                        knownValues.put(key, val);
                        knownValuesChanged = true;
                    } else if (option < 30) {                                       // getAtLeast
                        min = this.rb(2, true);
                        pair = tx.getAtLeast(min);
                        this.log("getAtLeast: " + s(min) + " -> " + s(pair));
                        if (pair == null) {
                            Assert.assertTrue(knownValues.tailMap(min).isEmpty(),
                              this + ": getAtLeast(" + s(min) + ") returned null but knownValues has " + knownValuesView);
                        } else if (knownValues.containsKey(pair.getKey()))
                            Assert.assertEquals(s(knownValues.get(pair.getKey())), s(pair.getValue()));
                        else {
                            knownValues.put(pair.getKey(), pair.getValue());
                            knownValuesChanged = true;
                        }
                    } else if (option < 40) {                                       // getAtMost
                        max = this.rb(2, true);
                        pair = tx.getAtMost(max);
                        this.log("getAtMost: " + s(max) + " -> " + s(pair));
                        if (pair == null) {
                            Assert.assertTrue(knownValues.headMap(max).isEmpty(),
                              this + ": getAtMost(" + s(max) + ") returned null but knownValues has " + knownValuesView);
                        } else if (knownValues.containsKey(pair.getKey()))
                            Assert.assertEquals(s(knownValues.get(pair.getKey())), s(pair.getValue()));
                        else {
                            knownValues.put(pair.getKey(), pair.getValue());
                            knownValuesChanged = true;
                        }
                    } else if (option < 50) {                                       // remove
                        key = this.rb(2, false);
                        if (this.r(5) == 0 && (pair = tx.getAtLeast(this.rb(1, false))) != null)
                            key = pair.getKey();
                        this.log("remove: " + s(key));
                        tx.remove(key);
                        knownValues.remove(key);
                        knownValuesChanged = true;
                    } else if (option < 52) {                                       // removeRange
                        min = this.rb2(2, 20);
                        do {
                            max = this.rb2(2, 30);
                        } while (max != null && min != null && ByteUtil.COMPARATOR.compare(min, max) > 0);
                        this.log("removeRange: " + s(min) + " to " + s(max));
                        tx.removeRange(min, max);
                        if (min == null && max == null)
                            knownValues.clear();
                        else if (min == null)
                            knownValues.headMap(max).clear();
                        else if (max == null)
                            knownValues.tailMap(min).clear();
                        else
                            knownValues.subMap(min, max).clear();
                        knownValuesChanged = true;
                    } else if (option < 60) {                                       // adjustCounter
                        key = this.rb(1, false);
                        key[0] = (byte)(key[0] & 0x0f);
                        val = tx.get(key);
                        long counter = -1;
                        if (val != null) {
                            try {
                                counter = tx.decodeCounter(val);
                                this.log("adj: valid value " + s(val) + " (" + counter + ") at key " + s(key));
                            } catch (IllegalArgumentException e) {
                                this.log("adj: bogus value " + s(val) + " at key " + s(key));
                                val = null;
                            }
                        }
                        if (val == null) {
                            counter = this.random.nextLong();
                            final byte[] encodedCounter = tx.encodeCounter(counter);
                            tx.put(key, encodedCounter);
                            this.log("adj: initialize " + s(key) + " to " + s(encodedCounter));
                        }
                        final long adj = this.random.nextInt(1 << this.random.nextInt(24)) - 1024;
                        final byte[] encodedCounter = tx.encodeCounter(counter + adj);
                        this.log("adj: " + s(key) + " by " + adj + " -> should now be " + s(encodedCounter));
                        tx.adjustCounter(key, adj);
                        knownValues.put(key, encodedCounter);
                        knownValuesChanged = true;
                    } else {                                                        // sleep
                        final int millis = this.r(50);
                        this.log("sleep " + millis + "ms");
                        try {
                            Thread.sleep(millis);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                    if (knownValuesChanged)
                        this.log("new knownValues: " + knownValuesView);
                }

                // TODO: keep track of removes as well as knownValues
                for (Map.Entry<byte[], byte[]> entry : knownValues.entrySet()) {
                    final byte[] key = entry.getKey();
                    final byte[] val = entry.getValue();
                    final byte[] txVal = tx.get(key);
                    Assert.assertEquals(txVal, val, this + ": tx has " + s(txVal) + " for key " + s(key)
                      + " but knownValues has:\n*** KNOWN VALUES: " + knownValuesView);
                }

                // Maybe commit
                final boolean rollback = this.r(5) == 3;
                this.log("about to " + (rollback ? "rollback" : "commit") + ":\n   KNOWN: "
                  + knownValuesView + "\n  COMMITTED: " + committedDataView);
                if (rollback) {
                    tx.rollback();
                    committed = false;
                    this.log("rolled-back");
                } else {
                    tx.commit();
                    committed = true;
                    this.log("committed");
                }

            } catch (TransactionTimeoutException e) {
                this.log("got " + e);
                committed = false;
            } catch (RetryTransactionException e) {             // might have committed, might not have, we don't know for sure
                this.log("got " + e);
            }

            // Doing this should always be allowed and shouldn't affect anything
            tx.rollback();

            // Verify committed database contents are now equal to what's expected
            if (this.committedData != null) {

                // Read actual content
                final TreeMap<byte[], byte[]> actual = new TreeMap<byte[], byte[]>(ByteUtil.COMPARATOR);
                final NavigableMap<String, String> actualView = this.stringView(actual);
                actual.putAll(this.readDatabase());

                // Update what we think is in the database and then compare to actual content
                if (Boolean.TRUE.equals(committed)) {

                    // Verify
                    this.log("tx was definitely committed");
                    Assert.assertEquals(actualView, knownValuesView,
                      this + "\n*** ACTUAL:\n" + actualView + "\n*** EXPECTED:\n" + knownValuesView + "\n");
                } else if (Boolean.FALSE.equals(committed)) {

                    // Verify
                    this.log("tx was definitely rolled back");
                    Assert.assertEquals(actualView, committedDataView,
                      this + "\n*** ACTUAL:\n" + actualView + "\n*** EXPECTED:\n" + committedDataView + "\n");
                } else {

                    // We don't know whether transaction got committed or not .. check both possibilities
                    final boolean matchCommit = actualView.equals(knownValuesView);
                    final boolean matchRollback = actualView.equals(committedDataView);
                    this.log("tx was either committed (" + matchCommit + ") or rolled back (" + matchRollback + ")");

                    // Verify one or the other
                    assert matchCommit || matchRollback :
                      this + "\n*** ACTUAL:\n" + actualView
                      + "\n*** COMMIT:\n" + knownValuesView
                      + "\n*** ROLLBACK:\n" + committedDataView + "\n";
                    committed = matchCommit;
                }

                // Update model of database
                if (committed) {
                    this.committedData.clear();
                    this.committedData.putAll(knownValues);
                }
            }
        }

        private NavigableMap<String, String> stringView(NavigableMap<byte[], byte[]> byteMap) {
            if (byteMap == null)
                return null;
            return new ConvertedNavigableMap<String, String, byte[], byte[]>(byteMap, this.converter, this.converter);
        }

        private TreeMap<byte[], byte[]> readDatabase() {
            return KVDatabaseTest.this.try3times(this.store, new Transactional<TreeMap<byte[], byte[]>>() {
                @Override
                public TreeMap<byte[], byte[]> transact(KVTransaction tx) {
                    return RandomTask.this.readDatabase(tx);
                }
            });
        }

        private TreeMap<byte[], byte[]> readDatabase(KVTransaction tx) {
            final TreeMap<byte[], byte[]> values = new TreeMap<byte[], byte[]>(ByteUtil.COMPARATOR);
            final Iterator<KVPair> i = tx.getRange(null, null, false);
            while (i.hasNext()) {
                final KVPair pair = i.next();
                values.put(pair.getKey(), pair.getValue());
            }
            if (i instanceof AutoCloseable) {
                try {
                    ((AutoCloseable)i).close();
                } catch (Exception e) {
                    // ignore
                }
            }
            return values;
        }

        private void log(String s) {
            if (KVDatabaseTest.this.log.isTraceEnabled())
                KVDatabaseTest.this.log.trace("Random[" + this.id + "]: " + s);
        }

        private int r(int max) {
            return this.random.nextInt(max);
        }

        private byte[] rb(int len, boolean allowFF) {
            final byte[] b = new byte[this.r(len) + 1];
            this.random.nextBytes(b);
            if (!allowFF && b[0] == (byte)0xff)
                b[0] = (byte)random.nextInt(0xff);
            return b;
        }

        private byte[] rb2(int len, int nullchance) {
            if (this.r(nullchance) == 0)
                return null;
            return this.rb(len, true);
        }

        @Override
        public String toString() {
            return "Thread[" + this.getName() + "]";
        }
    }

// Reader

    public class Reader implements Callable<byte[]> {

        final KVTransaction tx;
        final byte[] key;
        final boolean range;

        public Reader(KVTransaction tx, byte[] key, boolean range) {
            this.tx = tx;
            this.key = key;
            this.range = range;
        }

        public Reader(KVTransaction tx, byte[] key) {
            this(tx, key, false);
        }

        @Override
        public byte[] call() {
            if (this.range) {
                if (KVDatabaseTest.this.log.isTraceEnabled())
                    KVDatabaseTest.this.log.trace("reading at least " + s(this.key) + " in " + this.tx);
                final KVPair pair = this.tx.getAtLeast(this.key);
                KVDatabaseTest.this.log.info("finished reading at least " + s(this.key) + " -> " + pair + " in " + this.tx);
                return pair != null ? pair.getValue() : null;
            } else {
                if (KVDatabaseTest.this.log.isTraceEnabled())
                    KVDatabaseTest.this.log.trace("reading " + s(this.key) + " in " + this.tx);
                final byte[] value = this.tx.get(this.key);
                KVDatabaseTest.this.log.info("finished reading " + s(this.key) + " -> " + s(value) + " in " + this.tx);
                return value;
            }
        }
    }

// Writer

    public class Writer implements Runnable {

        final KVTransaction tx;
        final byte[] key;
        final byte[] value;

        public Writer(KVTransaction tx, byte[] key, byte[] value) {
            this.tx = tx;
            this.key = key;
            this.value = value;
        }

        @Override
        public void run() {
            try {
                KVDatabaseTest.this.log.info("putting " + s(this.key) + " -> " + s(this.value) + " in " + this.tx);
                this.tx.put(this.key, this.value);
            } catch (RuntimeException e) {
                KVDatabaseTest.this.log.info("exception putting " + s(this.key) + " -> " + s(this.value)
                  + " in " + this.tx + ": " + e);
                if (KVDatabaseTest.this.log.isTraceEnabled()) {
                    KVDatabaseTest.this.log.trace(this.tx + " put " + s(this.key) + " -> " + s(this.value)
                      + " failure exception trace:", e);
                }
                throw e;
            }
        }
    }

// Committer

    public class Committer implements Runnable {

        final KVTransaction tx;

        public Committer(KVTransaction tx) {
            this.tx = tx;
        }

        @Override
        public void run() {
            try {
                KVDatabaseTest.this.log.info("committing " + this.tx);
                this.tx.commit();
            } catch (RuntimeException e) {
                KVDatabaseTest.this.log.info("exception committing " + this.tx + ": " + e);
                if (KVDatabaseTest.this.log.isTraceEnabled())
                    KVDatabaseTest.this.log.trace(this.tx + " commit failure exception trace:", e);
                throw e;
            }
        }
    }
}

