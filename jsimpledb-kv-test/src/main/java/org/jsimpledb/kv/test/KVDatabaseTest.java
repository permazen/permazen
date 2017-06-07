
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.test;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.TransactionTimeoutException;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.CloseableIterator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public abstract class KVDatabaseTest extends KVTestSupport {

    protected ExecutorService executor;

    private final AtomicInteger numTransactionAttempts = new AtomicInteger();
    private final AtomicInteger numTransactionRetries = new AtomicInteger();

    private TreeMap<String, AtomicInteger> retryReasons = new TreeMap<>();

    @BeforeClass(dependsOnGroups = "configure")
    public void setup() throws Exception {
        this.executor = Executors.newFixedThreadPool(33);
        for (KVDatabase[] kvdb : this.getDBs()) {
            if (kvdb.length > 0)
                kvdb[0].start();
        }
        this.numTransactionAttempts.set(0);
        this.numTransactionRetries.set(0);
    }

    @AfterClass
    public void teardown() throws Exception {
        this.executor.shutdown();
        for (KVDatabase[] kvdb : this.getDBs()) {
            if (kvdb.length > 0)
                kvdb[0].stop();
        }
        final double retryRate = (double)this.numTransactionRetries.get() / (double)this.numTransactionAttempts.get();
        this.log.info("\n\n****************\n");
        this.log.info(String.format("Retry rate: %.2f%% (%d / %d)",
          retryRate * 100.0, this.numTransactionRetries.get(), this.numTransactionAttempts.get()));
        this.log.info("Retry reasons:");
        for (Map.Entry<String, AtomicInteger> entry : this.retryReasons.entrySet())
            this.log.info(String.format("%10d %s", entry.getValue().get(), entry.getKey()));
        this.log.info("\n\n****************\n");
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
        this.tryNtimes(store, tx -> tx.removeRange(null, null));
        this.log.info("testSimpleStuff() on " + store + ": done clearing database");

        // Verify database is empty
        this.log.info("testSimpleStuff() on " + store + ": verifying database is empty");
        this.tryNtimes(store, tx ->  {
            KVPair p = tx.getAtLeast(null, null);
            Assert.assertNull(p);
            p = tx.getAtMost(null, null);
            Assert.assertNull(p);
            try (CloseableIterator<KVPair> it = tx.getRange(null, null, false)) {
                Assert.assertFalse(it.hasNext());
            }
        });
        this.log.info("testSimpleStuff() on " + store + ": done verifying database is empty");

        // tx 1
        this.log.info("testSimpleStuff() on " + store + ": starting tx1");
        this.tryNtimes(store, tx -> {
            final byte[] x = tx.get(b("01"));
            if (x != null)
                Assert.assertEquals(tx.get(b("01")), b("02"));          // transaction was retried even though it succeeded
            tx.put(b("01"), b("02"));
            Assert.assertEquals(tx.get(b("01")), b("02"));
        });
        this.log.info("testSimpleStuff() on " + store + ": committed tx1");

        // tx 2
        this.log.info("testSimpleStuff() on " + store + ": starting tx2");
        this.tryNtimes(store, tx -> {
            final byte[] x = tx.get(b("01"));
            Assert.assertNotNull(x);
            Assert.assertTrue(Arrays.equals(x, b("02")) || Arrays.equals(x, b("03")));
            tx.put(b("01"), b("03"));
            Assert.assertEquals(tx.get(b("01")), b("03"));
        });
        this.log.info("testSimpleStuff() on " + store + ": committed tx2");

        // tx 3
        this.log.info("testSimpleStuff() on " + store + ": starting tx3");
        this.tryNtimes(store, tx -> {
            final byte[] x = tx.get(b("01"));
            Assert.assertEquals(x, b("03"));
            tx.put(b("10"), b("01"));
        });
        this.log.info("testSimpleStuff() on " + store + ": committed tx3");

        // Check stale access
        this.log.info("testSimpleStuff() on " + store + ": checking stale access");
        final KVTransaction tx = this.tryNtimesWithResult(store, tx1 -> tx1);
        try {
            tx.get(b("01"));
            assert false;
        } catch (StaleTransactionException e) {
            // expected
        }
        this.log.info("finished testSimpleStuff() on " + store);

        // Check read-only transaction (part 1)
        this.log.info("testSimpleStuff() on " + store + ": starting tx4");
        this.tryNtimes(store, kvt -> {
            kvt.setReadOnly(false);
            kvt.put(b("10"), b("dd"));
            final byte[] x = kvt.get(b("10"));
            Assert.assertEquals(x, b("dd"));
        });
        this.log.info("testSimpleStuff() on " + store + ": committed tx4");

        // Check read-only transaction (part 2)
        this.log.info("testSimpleStuff() on " + store + ": starting tx5");
        this.tryNtimes(store, kvt -> {
            kvt.setReadOnly(true);
            final byte[] x = kvt.get(b("10"));
            Assert.assertEquals(x, b("dd"));
            kvt.put(b("10"), b("ee"));
            final byte[] y = kvt.get(b("10"));
            Assert.assertEquals(y, b("ee"));
        });
        this.log.info("testSimpleStuff() on " + store + ": committed tx5");

        // Check read-only transaction (part 3)
        this.log.info("testSimpleStuff() on " + store + ": starting tx6");
        this.tryNtimes(store, kvt -> {
            kvt.setReadOnly(true);
            final byte[] x = kvt.get(b("10"));
            Assert.assertEquals(x, b("dd"));
        });
        this.log.info("testSimpleStuff() on " + store + ": committed tx6");
    }

    @Test(dataProvider = "kvdbs")
    public void testReadOnly(KVDatabase store) throws Exception {

        // Debug
        this.log.info("starting testReadOnly() on " + store);

        // Put some data in database
        this.log.info("testReadOnly() on " + store + ": initializing database");
        this.tryNtimes(store, tx -> {
            tx.removeRange(null, null);
            tx.put(ByteUtil.EMPTY, ByteUtil.EMPTY);
        });
        this.log.info("testReadOnly() on " + store + ": done initializing database");

        // Test we can set a transaction to read-only mode after reading some data
        this.log.info("testReadOnly() on " + store + ": testing setReadOnly()");
        this.tryNtimes(store, tx -> {
            final byte[] value1 = tx.get(ByteUtil.EMPTY);
            Assert.assertEquals(value1, ByteUtil.EMPTY);
            tx.setReadOnly(true);
            final byte[] value2 = tx.get(ByteUtil.EMPTY);
            Assert.assertEquals(value2, ByteUtil.EMPTY);
            Assert.assertTrue(tx.isReadOnly());
        });
        this.log.info("testReadOnly() on " + store + ": done testing setReadOnly()");
    }

    private byte[] randomBytes(int index) {
        final byte[] array = new byte[this.random.nextInt(10) + 1];
        this.random.nextBytes(array);
        array[0] = (byte)index;
        return array;
    }

    @Test(dataProvider = "kvdbs")
    public void testSortOrder(KVDatabase store) throws Exception {
        int index = 0;
        final byte[][][] pairs = new byte[][][] {
            { b(""),        this.randomBytes(index++) },
            { b("00"),      this.randomBytes(index++) },
            { b("0000"),    this.randomBytes(index++) },
            { b("0001"),    this.randomBytes(index++) },
            { b("000100"),  this.randomBytes(index++) },
            { b("007f"),    this.randomBytes(index++) },
            { b("007f00"),  this.randomBytes(index++) },
            { b("0080"),    this.randomBytes(index++) },
            { b("008000"),  this.randomBytes(index++) },
            { b("0081"),    this.randomBytes(index++) },
            { b("008100"),  this.randomBytes(index++) },
            { b("00ff"),    this.randomBytes(index++) },
            { b("00ff00"),  this.randomBytes(index++) },
            { b("0101"),    this.randomBytes(index++) },
            { b("0201"),    this.randomBytes(index++) },
            { b("7e01"),    this.randomBytes(index++) },
            { b("7f01"),    this.randomBytes(index++) },
            { b("8001"),    this.randomBytes(index++) },
            { b("fe01"),    this.randomBytes(index++) },
            { b("feff"),    this.randomBytes(index++) },
        };

        // Debug
        this.log.info("starting testSortOrder() on " + store);

        // Clear database
        this.log.info("testSortOrder() on " + store + ": clearing database");
        this.tryNtimes(store, tx -> tx.removeRange(null, null));
        this.log.info("testSortOrder() on " + store + ": done clearing database");

        // Write data
        this.log.info("testSortOrder() on " + store + ": writing data");
        this.tryNtimes(store, tx -> {
            for (byte[][] pair : pairs)
                tx.put(pair[0], pair[1]);
        });

        // Verify data
        this.log.info("testSortOrder() on " + store + ": verifying data order");
        this.tryNtimes(store, tx -> {
            try (final CloseableIterator<KVPair> i = tx.getRange(null, null, false)) {
                int index2 = 0;
                while (i.hasNext()) {
                    final byte[][] pair = pairs[index2];
                    final KVPair kvpair = i.next();
                    Assert.assertTrue(Arrays.equals(kvpair.getKey(), pair[0]) && Arrays.equals(kvpair.getValue(), pair[1]),
                      String.format("expected pair { %s, %s } but got { %s, %s } at index %d",
                       ByteUtil.toString(pair[0]), ByteUtil.toString(pair[1]),
                       ByteUtil.toString(kvpair.getKey()), ByteUtil.toString(kvpair.getValue()),
                       index2));
                    index2++;
                }
            }
        });
    }

    @Test(dataProvider = "kvdbs")
    public void testKeyWatch(KVDatabase store) throws Exception {

        // Debug
        this.log.info("starting testKeyWatch() on " + store);

        // Clear database
        this.log.info("testKeyWatch() on " + store + ": clearing database");
        this.tryNtimes(store, tx -> tx.removeRange(null, null));
        this.log.info("testKeyWatch() on " + store + ": done clearing database");

        // Set up the modifications we want to test
        final ArrayList<Consumer<KVTransaction>> mods = new ArrayList<>();
        mods.add(tx -> tx.put(b("0123"), b("4567")));
        mods.add(tx -> tx.put(b("0123"), b("89ab")));
        mods.add(tx -> tx.put(b("0123"), tx.encodeCounter(1234)));
        mods.add(tx -> tx.adjustCounter(b("0123"), 99));
        mods.add(tx -> tx.removeRange(b("01"), b("02")));
        mods.add(tx -> tx.put(b("0123"), b("")));
        mods.add(tx -> tx.remove(b("0123")));

        // Set watches, perform modifications, and test notifications
        for (Consumer<KVTransaction> mod : mods) {

            // Set watch
            this.log.info("testKeyWatch() on " + store + ": creating key watch for " + mod);
            final Future<Void> watch = this.tryNtimesWithResult(store, tx -> {
                try {
                    return tx.watchKey(b("0123"));
                } catch (UnsupportedOperationException e) {
                    return null;
                }
            });
            if (watch == null) {
                this.log.info("testKeyWatch() on " + store + ": key watches not supported, bailing out");
                return;
            }
            this.log.info("testKeyWatch() on " + store + ": created key watch: " + watch);

            // Perform modification
            this.log.info("testKeyWatch() on " + store + ": testing " + mod);
            this.tryNtimes(store, mod);

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
        this.tryNtimes(store, tx -> tx.removeRange(null, null));

        // Both read the same key
        final KVTransaction[] txs = new KVTransaction[] {
            this.createKVTransaction(store),
            this.createKVTransaction(store)
        };
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
                } catch (AssertionError e) {
                    throw e;
                } catch (Throwable e) {
                    while (e instanceof ExecutionException)
                        e = e.getCause();
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
            final KVTransaction tx2 = this.createKVTransaction(store);
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
        this.tryNtimes(store, tx -> tx.removeRange(null, null));

        // Multiple concurrent transactions with overlapping read ranges and non-intersecting write ranges
        int done = 0;
        KVTransaction[] txs = new KVTransaction[this.getNonconflictingTransactionCount()];
        for (int i = 0; i < txs.length; i++)
            txs[i] = this.createKVTransaction(store);
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
                            txs[i] = this.createKVTransaction(store);
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
                    this.numTransactionAttempts.incrementAndGet();
                    txs[i].commit();
                } catch (RetryTransactionException e) {
                    this.updateRetryStats(e);
                    txs[i] = this.createKVTransaction(store);
                    continue;
                }
                txs[i] = null;
            }
        }
        this.log.info("finished testNonconflictingTransactions() on " + store);
    }

    protected int getNonconflictingTransactionCount() {
        return 10;
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
        this.testParallelTransactions(new KVDatabase[] { store });
    }

    public void testParallelTransactions(KVDatabase[] stores) throws Exception {
        this.log.info("starting testParallelTransactions() on " + Arrays.asList(stores));
        for (int count = 0; count < this.getParallelTransactionLoopCount(); count++) {
            this.log.info("starting testParallelTransactions() iteration " + count);
            final RandomTask[] tasks = new RandomTask[this.getParallelTransactionTaskCount()];
            for (int i = 0; i < tasks.length; i++) {
                tasks[i] = new RandomTask(i, stores[this.random.nextInt(stores.length)], this.random.nextLong());
                tasks[i].start();
            }
            for (RandomTask task : tasks)
                task.join();
            for (int i = 0; i < tasks.length; i++) {
                final Throwable fail = tasks[i].getFail();
                if (fail != null)
                    throw new Exception("task #" + i + " failed: >>>" + this.show(fail).trim() + "<<<");
            }
            this.log.info("finished testParallelTransactions() iteration " + count);
        }
        this.log.info("finished testParallelTransactions() on " + Arrays.asList(stores));
        for (KVDatabase store : stores) {
            if (store instanceof Closeable)
                ((Closeable)store).close();
        }
    }

    protected int getParallelTransactionLoopCount() {
        return 25;
    }

    protected int getParallelTransactionTaskCount() {
        return 25;
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
        this.tryNtimes(store, tx -> tx.removeRange(null, null));

        // Keep an in-memory record of what is in the committed database
        final TreeMap<byte[], byte[]> committedData = new TreeMap<>(ByteUtil.COMPARATOR);

        // Run transactions
        for (int i = 0; i < this.getSequentialTransactionLoopCount(); i++) {
            final RandomTask task = new RandomTask(i, store, committedData, this.random.nextLong());
            task.run();
            final Throwable fail = task.getFail();
            if (fail != null)
                throw new Exception("task #" + i + " failed: >>>" + this.show(fail).trim() + "<<<");
        }
        this.log.info("finished testSequentialTransactions() on " + store);
    }

    protected int getSequentialTransactionLoopCount() {
        return 50;
    }

    /**
     * This test has multiple threads banging away on a single transaction to verify
     * that the transaction is thread safe.
     *
     * @param store underlying store
     * @throws Exception if an error occurs
     */
    @Test(dataProvider = "kvdbs")
    public void testMultipleThreadsTransaction(KVDatabase store) throws Exception {
        this.log.info("starting testMultipleThreadsTransaction() on " + store);

        // Clear database
        this.tryNtimes(store, tx -> tx.removeRange(null, null));

        // Populate database with some data
        final TreeMap<byte[], byte[]> committedData = new TreeMap<>(ByteUtil.COMPARATOR);
        final RandomTask populateTask = new RandomTask(-1, store, committedData, this.random.nextLong());
        this.log.info("testMultipleThreadsTransaction() populating database with random data");
        populateTask.run();
        Throwable fail = populateTask.getFail();
        if (fail != null)
            throw new Exception("populate task failed: >>>" + this.show(fail).trim() + "<<<");

        // Create new transaction and blast away at it
        this.log.info("testMultipleThreadsTransaction() starting threads");
        this.tryNtimes(store, tx -> {

            // Create worker threads
            final RandomTask[] tasks = new RandomTask[33];
            final Thread[] threads = new Thread[tasks.length];
            for (int i = 0; i < tasks.length; i++) {
                final RandomTask task = new RandomTask(i, null, KVDatabaseTest.this.random.nextLong());
                threads[i] = new Thread(() -> task.runRandomAccess(tx));
                tasks[i] = task;
            }

            // Start threads
            for (int i = 0; i < tasks.length; i++)
                threads[i].start();

            // Join threads
            for (int i = 0; i < tasks.length; i++) {
                try {
                    threads[i].join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            // Check for errors
            for (int i = 0; i < tasks.length; i++) {
                final Throwable taskFail = tasks[i].getFail();
                if (taskFail != null) {
                    throw new RuntimeException("task #" + i + " failed: >>>"
                      + KVDatabaseTest.this.show(taskFail).trim() + "<<<");
                }
            }
        });
        this.log.info("finished testMultipleThreadsTransaction() on " + store);
    }

    protected void tryNtimes(KVDatabase kvdb, Consumer<KVTransaction> consumer) {
        this.<Void>tryNtimesWithResult(kvdb, kvt -> {
            consumer.accept(kvt);
            return null;
        });
    }

    protected <R> R tryNtimesWithResult(KVDatabase kvdb, Function<KVTransaction, R> function) {
        RetryTransactionException retry = null;
        for (int count = 0; count < this.getNumTries(); count++) {
            try {
                this.numTransactionAttempts.incrementAndGet();
                final KVTransaction tx = kvdb.createTransaction();
                final R result = function.apply(tx);
                tx.commit();
                return result;
            } catch (RetryTransactionException e) {
                this.updateRetryStats(e);
                KVDatabaseTest.this.log.debug("attempt #" + (count + 1) + " yeilded " + e);
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

    protected int getNumTries() {
        return 3;
    }

    // Some k/v databases can throw a RetryTransaction from createTransaction()
    protected KVTransaction createKVTransaction(KVDatabase kvdb) {
        RetryTransactionException retry = null;
        for (int count = 0; count < this.getNumTries(); count++) {
            try {
                return kvdb.createTransaction();
            } catch (RetryTransactionException e) {
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

    protected void updateRetryStats(RetryTransactionException e) {
        this.numTransactionRetries.incrementAndGet();
        String message = e.getMessage();
        if (message != null)
            message = this.mapRetryExceptionMessage(message);
        synchronized (this) {
            AtomicInteger counter = this.retryReasons.get(message);
            if (counter == null) {
                counter = new AtomicInteger();
                this.retryReasons.put(message, counter);
            }
            counter.incrementAndGet();
        }
    }

    protected String mapRetryExceptionMessage(String message) {
        return message.replaceAll("[0-9]+", "NNN");
    }

// RandomTask

    public class RandomTask extends Thread {

        private final int id;
        private final KVDatabase store;
        private final Random random;
        private final TreeMap<byte[], byte[]> committedData;            // tracks actual committed data, if known
        private final NavigableMap<String, String> committedDataView;
        private final ArrayList<String> log = new ArrayList<>(KVDatabaseTest.this.getRandomTaskMaxIterations());

        private Throwable fail;

        public RandomTask(int id, KVDatabase store, long seed) {
            this(id, store, null, seed);
        }

        public RandomTask(int id, KVDatabase store, TreeMap<byte[], byte[]> committedData, long seed) {
            super("Random[" + id + "," + (committedData != null ? "SEQ" : "PAR") + "]");
            this.id = id;
            this.store = store;
            this.committedData = committedData;
            this.committedDataView = stringView(this.committedData);
            this.random = new Random(seed);
            this.log("seed = " + seed);
        }

        @Override
        public void run() {
            KVDatabaseTest.this.log.debug("*** " + this + " STARTING");
            try {
                this.test();
                this.log("succeeded");
            } catch (Throwable t) {
                final StringWriter buf = new StringWriter();
                t.printStackTrace(new PrintWriter(buf, true));
                this.log("failed: " + t + "\n" + buf.toString());
                this.fail = t;
            } finally {
                KVDatabaseTest.this.log.debug("*** " + this + " FINISHED");
                this.dumpLog(this.fail != null);
            }
        }

        public Throwable getFail() {
            return this.fail;
        }

        @SuppressWarnings("unchecked")
        private void test() throws Exception {

            // Keep track of key/value pairs that we know should exist in the transaction
            final TreeMap<byte[], byte[]> knownValues = new TreeMap<>(ByteUtil.COMPARATOR);
            final NavigableMap<String, String> knownValuesView = stringView(knownValues);
            final TreeSet<byte[]> putValues = new TreeSet<>(ByteUtil.COMPARATOR);
            final NavigableSet<String> putValuesView = stringView(putValues);

            // Keep track of known empty ranges
            final KeyRanges knownEmpty = new KeyRanges();

            // Load actual committed database contents (if known) into "known values" tracker
            if (this.committedData != null)
                knownValues.putAll(this.committedData);

            // Verify committed data is accurate
            if (this.committedData != null)
                Assert.assertEquals(stringView(this.readDatabase()), knownValuesView);

            // Create transaction; every now and then, do transaction read-only
            final KVTransaction tx = KVDatabaseTest.this.createKVTransaction(this.store);
            final boolean readOnly = this.r(100) < 3;
            KVDatabaseTest.this.log.debug("*** CREATED " + (readOnly ? "R/O" : "R/W") + " TX " + tx);
            assert !tx.isReadOnly();
            if (readOnly) {
                tx.setReadOnly(true);
                assert tx.isReadOnly();
            }

            // Save a copy of committed data
            final TreeMap<byte[], byte[]> previousCommittedData = this.committedData != null ?
              (TreeMap<byte[], byte[]>)this.committedData.clone() : null;
            //final NavigableMap<String, String> previousCommittedDataView = stringView(previousCommittedData);

            // Verify committed data is still accurate
            if (this.committedData != null && this.random.nextInt(8) == 3)
                Assert.assertEquals(stringView(this.readDatabase()), knownValuesView);

            // Note: if this.committedData != null, then knownValues will exactly track the transaction, otherwise,
            // knownValues only contains values we know are in there; nothing is known about uncontained values.

            // Make a bunch of random changes
            Boolean committed = null;
            try {
                final int limit = this.r(KVDatabaseTest.this.getRandomTaskMaxIterations());
                for (int j = 0; j < limit; j++) {
                    byte[] key;
                    byte[] val;
                    byte[] min;
                    byte[] max;
                    KVPair pair;
                    int option = this.r(62);
                    boolean knownValuesChanged = false;
                    if (option < 10) {                                              // get
                        key = this.rb(1, false);
                        val = tx.get(key);
                        this.log("get: " + s(key) + " -> " + s(val));
                        if (val == null) {
                            assert !knownValues.containsKey(key) :
                              this + ": get(" + s(key) + ") returned null but"
                              + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView
                              + "\n  emptys=" + knownEmpty + "\n  tx=" + this.toString(tx);
                            knownEmpty.add(new KeyRange(key));
                            knownValuesChanged = true;
                        } else if (knownValues.containsKey(key)) {
                            assert s(knownValues.get(key)).equals(s(val)) :
                              this + ": get(" + s(key) + ") returned " + s(val) + " but"
                              + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView + "\n  emptys="
                              + "\n  emptys=" + knownEmpty + "\n  tx=" + this.toString(tx);
                        } else {
                            knownValues.put(key, val);
                            knownValuesChanged = true;
                        }
                        assert val == null || !knownEmpty.contains(key) :
                          this + ": get(" + s(key) + ") returned " + s(val) + " but"
                          + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView
                          + "\n  emptys=" + knownEmpty + "\n  tx=" + this.toString(tx);
                    } else if (option < 20) {                                       // put
                        key = this.rb(1, false);
                        val = this.rb(2, true);
                        this.log("put: " + s(key) + " -> " + s(val));
                        tx.put(key, val);
                        knownValues.put(key, val);
                        putValues.add(key);
                        knownEmpty.remove(new KeyRange(key));
                        knownValuesChanged = true;
                    } else if (option < 30) {                                       // getAtLeast
                        min = this.rb(1, true);
                        do {
                            max = this.rb2(this.r(2) + 1, 20);
                        } while (max != null && min != null && ByteUtil.COMPARATOR.compare(min, max) > 0);
                        pair = tx.getAtLeast(min, max);
                        this.log("getAtLeast: " + s(min) + "," + s(max) + " -> " + s(pair));
                        assert pair == null || ByteUtil.compare(pair.getKey(), min) >= 0;
                        assert pair == null || max == null || ByteUtil.compare(pair.getKey(), max) < 0;
                        if (pair == null) {
                            assert (max != null ? knownValues.subMap(min, max) : knownValues.tailMap(min)).isEmpty() :
                              this + ": getAtLeast(" + s(min) + "," + s(max) + ") returned " + null + " but"
                              + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView
                              + "\n  emptys=" + knownEmpty + "\n  tx=" + this.toString(tx);
                        } else if (knownValues.containsKey(pair.getKey())) {
                            assert s(knownValues.get(pair.getKey())).equals(s(pair.getValue())) :
                              this + ": getAtLeast(" + s(min) + "," + s(max) + ") returned " + pair + " but"
                              + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView
                              + "\n  emptys=" + knownEmpty + "\n  tx=" + this.toString(tx);
                        } else
                            knownValues.put(pair.getKey(), pair.getValue());
                        assert pair == null || !knownEmpty.contains(pair.getKey()) :
                          this + ": getAtLeast(" + s(min) + "," + s(max) + ") returned " + pair + " but"
                          + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView
                          + "\n  emptys=" + knownEmpty + "\n  tx=" + this.toString(tx);
                        knownEmpty.add(new KeyRange(min, pair != null ? pair.getKey() : max));
                        knownValuesChanged = true;
                    } else if (option < 40) {                                       // getAtMost
                        max = this.rb(1, true);
                        do {
                            min = this.rb2(this.r(2) + 1, 20);
                        } while (max != null && min != null && ByteUtil.COMPARATOR.compare(min, max) > 0);
                        pair = tx.getAtMost(max, min);
                        this.log("getAtMost: " + s(max) + "," + s(min) + " -> " + s(pair));
                        assert pair == null || min == null || ByteUtil.compare(pair.getKey(), min) >= 0;
                        assert pair == null || ByteUtil.compare(pair.getKey(), max) < 0;
                        if (pair == null) {
                            assert (min != null ? knownValues.subMap(min, max) : knownValues.headMap(max)).isEmpty() :
                              this + ": getAtMost(" + s(max) + "," + s(min) + ") returned " + null + " but"
                              + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView
                              + "\n  emptys=" + knownEmpty + "\n  tx=" + this.toString(tx);
                        } else if (knownValues.containsKey(pair.getKey())) {
                            assert s(knownValues.get(pair.getKey())).equals(s(pair.getValue())) :
                              this + ": getAtMost(" + s(max) + "," + s(min) + ") returned " + pair + " but"
                              + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView
                              + "\n  emptys=" + knownEmpty + "\n  tx=" + this.toString(tx);
                        } else
                            knownValues.put(pair.getKey(), pair.getValue());
                        assert pair == null || !knownEmpty.contains(pair.getKey()) :
                          this + ": getAtMost(" + s(max) + "," + s(min) + ") returned " + pair + " but"
                          + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView
                          + "\n  emptys=" + knownEmpty + "\n  tx=" + this.toString(tx);
                        knownEmpty.add(new KeyRange(
                          pair != null ? ByteUtil.getNextKey(pair.getKey()) : min != null ? min : ByteUtil.EMPTY,
                          max));
                        knownValuesChanged = true;
                    } else if (option < 50) {                                       // remove
                        key = this.rb(1, false);
                        if (this.r(5) == 0 && (pair = tx.getAtLeast(this.rb(1, false), null)) != null)
                            key = pair.getKey();
                        this.log("remove: " + s(key));
                        tx.remove(key);
                        knownValues.remove(key);
                        putValues.remove(key);
                        knownEmpty.add(new KeyRange(key));
                        knownValuesChanged = true;
                    } else if (option < 52) {                                       // removeRange
                        min = this.rb2(2, 20);
                        do {
                            max = this.rb2(2, 30);
                        } while (max != null && min != null && ByteUtil.COMPARATOR.compare(min, max) > 0);
                        this.log("removeRange: " + s(min) + " to " + s(max));
                        tx.removeRange(min, max);
                        if (min == null && max == null) {
                            knownValues.clear();
                            putValues.clear();
                        } else if (min == null) {
                            knownValues.headMap(max).clear();
                            putValues.headSet(max).clear();
                        } else if (max == null) {
                            knownValues.tailMap(min).clear();
                            putValues.tailSet(min).clear();
                        } else {
                            knownValues.subMap(min, max).clear();
                            putValues.subSet(min, max).clear();
                        }
                        knownEmpty.add(new KeyRange(min != null ? min : ByteUtil.EMPTY, max));
                        knownValuesChanged = true;
                    } else if (option < 60) {                                       // adjustCounter
                        key = this.rb(1, false);
                        key[0] = (byte)(key[0] & 0x0f);
                        val = tx.get(key);
                        long counter = -1;
                        if (val != null) {
                            try {
                                counter = tx.decodeCounter(val);
                                this.log("adj: found valid value " + s(val) + " (" + counter + ") at key " + s(key));
                            } catch (IllegalArgumentException e) {
                                this.log("adj: found bogus value " + s(val) + " at key " + s(key));
                                val = null;
                            }
                        }
                        if (val == null) {
                            counter = this.random.nextLong();
                            final byte[] encodedCounter = tx.encodeCounter(counter);
                            tx.put(key, encodedCounter);
                            putValues.add(key);
                            this.log("adj: initialize " + s(key) + " to " + s(encodedCounter));
                        }
                        final long adj = this.random.nextInt(1 << this.random.nextInt(24)) - 1024;
                        final byte[] encodedCounter = tx.encodeCounter(counter + adj);
                        this.log("adj: " + s(key) + " by " + adj + " -> should now be " + s(encodedCounter));
                        tx.adjustCounter(key, adj);
                        knownValues.put(key, encodedCounter);
                        knownEmpty.remove(new KeyRange(key));
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
                    if (knownValuesChanged) {
                        this.log("new values:"
                          + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView + "\n  emptys=" + knownEmpty);
                    }

                    // Verify everything we know to be there is there
                    for (Map.Entry<byte[], byte[]> entry : knownValues.entrySet()) {
                        final byte[] knownKey = entry.getKey();
                        final byte[] expected = entry.getValue();
                        final byte[] actual = tx.get(knownKey);
                        assert actual != null && ByteUtil.compare(actual, expected) == 0 :
                          this + ": tx has " + s(actual) + " for key " + s(knownKey) + " but"
                          + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView
                          + "\n  emptys=" + knownEmpty + "\n  tx=" + this.toString(tx);
                    }

                    // Verify everything we know to no be there is not there
                    try (final CloseableIterator<KVPair> iter = tx.getRange(null, null, false)) {
                        while (iter.hasNext()) {
                            pair = iter.next();
                            assert !knownEmpty.contains(pair.getKey()) :
                              this + ": tx contains " + pair + " but"
                              + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView
                              + "\n  emptys=" + knownEmpty + "\n  tx=" + this.toString(tx);
                        }
                    }
                }

                // Maybe commit
                final boolean rollback = this.r(5) == 3;
                KVDatabaseTest.this.log.debug("*** " + (rollback ? "ROLLING BACK" : "COMMITTING")
                  + (readOnly ? " R/O" : " R/W") + " TX " + tx);
                this.log("about to " + (rollback ? "rollback" : "commit") + ":"
                  + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView + "\n  emptys=" + knownEmpty
                  + "\n  committed: " + committedDataView);
                if (rollback) {
                    tx.rollback();
                    committed = false;
                    this.log("rolled-back");
                } else {
                    try {
                        KVDatabaseTest.this.numTransactionAttempts.incrementAndGet();
                        tx.commit();
                    } catch (RetryTransactionException e) {
                        KVDatabaseTest.this.updateRetryStats(e);
                        throw e;
                    }
                    committed = true;
                    KVDatabaseTest.this.log.debug("*** COMMITTED TX " + tx);
                    this.log("committed");
                }

            } catch (TransactionTimeoutException e) {
                KVDatabaseTest.this.log.debug("*** TX " + tx + " THREW " + e);
                this.log("got " + e);
                committed = false;
            } catch (RetryTransactionException e) {             // might have committed, might not have, we don't know for sure
                KVDatabaseTest.this.log.debug("*** TX " + tx + " THREW " + e);
                this.log("got " + e);
            }

            // Doing this should always be allowed and shouldn't affect anything
            tx.rollback();

            // Verify committed database contents are now equal to what's expected
            if (this.committedData != null) {

                // Read actual content
                final TreeMap<byte[], byte[]> actual = new TreeMap<>(ByteUtil.COMPARATOR);
                final NavigableMap<String, String> actualView = stringView(actual);
                actual.putAll(this.readDatabase());

                // Update what we think is in the database and then compare to actual content
                if (Boolean.TRUE.equals(committed) && !readOnly) {

                    // Verify
                    this.log("tx was definitely committed");
                    assert actualView.equals(knownValuesView) :
                      this + "\n*** ACTUAL:\n" + actualView + "\n*** EXPECTED:\n" + knownValuesView + "\n";
                } else if (Boolean.FALSE.equals(committed) || readOnly) {

                    // Verify
                    this.log("tx was definitely rolled back (committed=" + committed + ", readOnly=" + readOnly + ")");
                    assert actualView.equals(committedDataView) :
                      this + "\n*** ACTUAL:\n" + actualView + "\n*** EXPECTED:\n" + committedDataView + "\n";
                    committed = false;
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

        public void runRandomAccess(KVTransaction tx) {
            KVDatabaseTest.this.log.debug("*** " + this + " runRandomAccess() STARTING");
            try {
                this.performRandomAccess(tx);
            } catch (Throwable t) {
                this.fail = t;
            } finally {
                KVDatabaseTest.this.log.debug("*** " + this + " runRandomAccess() FINISHED");
            }
        }

        public void performRandomAccess(KVTransaction tx) {
            final int limit = this.r(KVDatabaseTest.this.getRandomTaskMaxIterations());
            for (int j = 0; j < limit; j++) {
                byte[] key;
                byte[] val;
                byte[] min;
                byte[] max;
                KVPair pair;
                int option = this.r(62);
                boolean knownValuesChanged = false;
                if (option < 10) {                                              // get
                    key = this.rb(1, false);
                    tx.get(key);
                } else if (option < 20) {                                       // put
                    key = this.rb(1, false);
                    val = this.rb(2, true);
                    tx.put(key, val);
                } else if (option < 30) {                                       // getAtLeast
                    min = this.rb(1, true);
                    do {
                        max = this.rb2(this.r(2) + 1, 20);
                    } while (max != null && min != null && ByteUtil.COMPARATOR.compare(min, max) > 0);
                    tx.getAtLeast(min, max);
                } else if (option < 40) {                                       // getAtMost
                    max = this.rb(1, true);
                    do {
                        min = this.rb2(this.r(2) + 1, 20);
                    } while (max != null && min != null && ByteUtil.COMPARATOR.compare(min, max) > 0);
                    tx.getAtMost(max, min);
                } else if (option < 50) {                                       // remove
                    key = this.rb(1, false);
                    if (this.r(5) == 0 && (pair = tx.getAtLeast(this.rb(1, false), null)) != null)
                        key = pair.getKey();
                    tx.remove(key);
                } else if (option < 52) {                                       // removeRange
                    min = this.rb2(2, 20);
                    do {
                        max = this.rb2(2, 30);
                    } while (max != null && min != null && ByteUtil.COMPARATOR.compare(min, max) > 0);
                    tx.removeRange(min, max);
                } else if (option < 60) {                                       // adjustCounter
                    key = this.rb(1, false);
                    final long adj = this.random.nextInt(1 << this.random.nextInt(24)) - 1024;
                    tx.adjustCounter(key, adj);
                } else {                                                        // yeild
                    try {
                        Thread.sleep(0, 1);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        }

        private TreeMap<byte[], byte[]> readDatabase() {
            return KVDatabaseTest.this.tryNtimesWithResult(this.store, RandomTask.this::readDatabase);
        }

        private TreeMap<byte[], byte[]> readDatabase(KVStore tx) {
            final TreeMap<byte[], byte[]> values = new TreeMap<>(ByteUtil.COMPARATOR);
            try (final CloseableIterator<KVPair> i = tx.getRange(null, null, false)) {
                while (i.hasNext()) {
                    final KVPair pair = i.next();
                    values.put(pair.getKey(), pair.getValue());
                }
            }
            return values;
        }

        private String toString(KVStore kv) {
            final StringBuilder buf = new StringBuilder();
            buf.append('{');
            try (final CloseableIterator<KVPair> i = kv.getRange(null, null, false)) {
                while (i.hasNext()) {
                    final KVPair pair = i.next();
                    if (buf.length() > 1)
                        buf.append(", ");
                    buf.append(ByteUtil.toString(pair.getKey())).append('=').append(ByteUtil.toString(pair.getValue()));
                }
            }
            buf.append('}');
            return buf.toString();
        }

        private void log(String s) {
            this.log.add(s);
        }

        private void dumpLog(boolean force) {
            if (!force && !KVDatabaseTest.this.log.isTraceEnabled())
                return;
            synchronized (KVDatabaseTest.this) {
                final StringBuilder buf = new StringBuilder(this.log.size() * 40);
                for (String s : this.log)
                    buf.append(s).append('\n');
                KVDatabaseTest.this.log.debug("*** BEGIN " + this + " LOG ***\n\n{}\n*** END " + this + " LOG ***", buf);
            }
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
            return "Random[" + this.id + "," + (this.committedData != null ? "SEQ" : "PAR") + "]";
        }
    }

    protected int getRandomTaskMaxIterations() {
        return 1000;
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
                final KVPair pair = this.tx.getAtLeast(this.key, null);
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
                KVDatabaseTest.this.numTransactionAttempts.incrementAndGet();
                this.tx.commit();
            } catch (RuntimeException e) {
                if (e instanceof RetryTransactionException)
                    KVDatabaseTest.this.updateRetryStats((RetryTransactionException)e);
                KVDatabaseTest.this.log.info("exception committing " + this.tx + ": " + e);
                if (KVDatabaseTest.this.log.isTraceEnabled())
                    KVDatabaseTest.this.log.trace(this.tx + " commit failure exception trace:", e);
                throw e;
            }
        }
    }
}

