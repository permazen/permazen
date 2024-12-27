
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.test;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KVTransactionTimeoutException;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.kv.util.MemoryKVStore;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public abstract class KVDatabaseTest extends KVTestSupport {

    protected ExecutorService executor;
    protected ExecutorService[] threads;

    @BeforeClass(dependsOnGroups = "configure")
    public void setupExecutorAndDatabases() throws Exception {
        this.executor = Executors.newFixedThreadPool(33);
        this.threads = new ExecutorService[Math.max(2, this.getNonconflictingTransactionCount())];
        for (int i = 0; i < this.threads.length; i++)
            this.threads[i] = Executors.newSingleThreadExecutor();
        for (KVDatabase[] kvdb : this.getDBs()) {
            if (kvdb.length > 0)
                kvdb[0].start();
        }
    }

    @AfterClass
    public void teardownExecutorAndDatabases() throws Exception {
        this.executor.shutdown();
        for (int i = 0; i < this.threads.length; i++)
            this.threads[i].shutdown();
        this.executor.awaitTermination(3, TimeUnit.SECONDS);
        for (int i = 0; i < this.threads.length; i++)
            this.threads[i].awaitTermination(3, TimeUnit.SECONDS);
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

    // How many transactions in testNonconflictingTransactions()?
    protected int getNonconflictingTransactionCount() {
        return 10;
    }

    // How many loops in testParallelTransactions()?
    protected int getParallelTransactionLoopCount() {
        return 25;
    }

    // How many transactions in testParallelTransactions()?
    protected int getParallelTransactionTaskCount() {
        return 25;
    }

    // How many transactions in testSequentialTransactions()?
    protected int getSequentialTransactionLoopCount() {
        return 50;
    }

    // How many iterations in RandomTask?
    protected int getRandomTaskMaxIterations() {
        return 1000;
    }

    // Does this database permit write skew anomalies?
    protected boolean allowsWriteSkewAnomaly() {
        return false;
    }

    // Does this database allow tx.setReadOnly() after data has been accessed?
    protected boolean supportsReadOnlyAfterDataAccess() {
        return true;
    }

    // When there's a conflict, is it OK if BOTH transactions fail?
    protected boolean allowBothTransactionsToFail() {
        return false;
    }

    // Does this database tolerate multiple threads accessing a single transaction?
    protected boolean transactionsAreThreadSafe() {
        return true;
    }

    // Does this database support multiple simulatenous write transactions?
    protected boolean supportsMultipleWriteTransactions() {
        return true;
    }

    @Test(dataProvider = "kvdbs")
    public void testSimpleStuff(KVDatabase store) throws Exception {

        // Debug
        this.log.info("starting testSimpleStuff() on {}", store);

        // Clear database
        this.log.info("testSimpleStuff() on {}: clearing database", store);
        this.tryNtimes(store, tx -> tx.removeRange(null, null));
        this.log.info("testSimpleStuff() on {}: done clearing database", store);

        // Verify database is empty
        this.log.info("testSimpleStuff() on {}: verifying database is empty", store);
        this.tryNtimes(store, tx ->  {
            KVPair p = tx.getAtLeast(null, null);
            Assert.assertNull(p);
            p = tx.getAtMost(null, null);
            Assert.assertNull(p);
            try (CloseableIterator<KVPair> it = tx.getRange(null, null, false)) {
                Assert.assertFalse(it.hasNext());
            }
        });
        this.log.info("testSimpleStuff() on {}: done verifying database is empty", store);

        // tx 1
        this.log.info("testSimpleStuff() on {}: starting tx1", store);
        this.tryNtimes(store, tx -> {
            final ByteData x = tx.get(b("01"));
            if (x != null)
                Assert.assertEquals(tx.get(b("01")), b("02"));          // transaction was retried even though it succeeded
            tx.put(b("01"), b("02"));
            Assert.assertEquals(tx.get(b("01")), b("02"));
        });
        this.log.info("testSimpleStuff() on {}: committed tx1", store);

        // tx 2
        this.log.info("testSimpleStuff() on {}: starting tx2", store);
        this.tryNtimes(store, tx -> {
            final ByteData x = tx.get(b("01"));
            Assert.assertNotNull(x);
            Assert.assertTrue(Objects.equals(x, b("02")) || Objects.equals(x, b("03")));
            tx.put(b("01"), b("03"));
            Assert.assertEquals(tx.get(b("01")), b("03"));
        });
        this.log.info("testSimpleStuff() on {}: committed tx2", store);

        // tx 3
        this.log.info("testSimpleStuff() on {}: starting tx3", store);
        this.tryNtimes(store, tx -> {
            final ByteData x = tx.get(b("01"));
            Assert.assertEquals(x, b("03"));
            tx.put(b("10"), b("01"));
        });
        this.log.info("testSimpleStuff() on {}: committed tx3", store);

        // Check stale access
        this.log.info("testSimpleStuff() on {}: checking stale access", store);
        final KVTransaction tx = this.tryNtimesWithResult(store, tx1 -> tx1);
        try {
            tx.get(b("01"));
            assert false;
        } catch (StaleKVTransactionException e) {
            // expected
        }
        this.log.info("finished testSimpleStuff() on {}", store);

        // Check read-only transaction (part 1)
        this.log.info("testSimpleStuff() on {}: starting tx4", store);
        this.tryNtimes(store, kvt -> {
            kvt.setReadOnly(false);
            kvt.put(b("10"), b("dd"));
            final ByteData x = kvt.get(b("10"));
            Assert.assertEquals(x, b("dd"));
        });
        this.log.info("testSimpleStuff() on {}: committed tx4", store);

        // Check read-only transaction (part 2)
        this.log.info("testSimpleStuff() on {}: starting tx5", store);
        this.tryNtimes(store, kvt -> {
            kvt.setReadOnly(true);
            final ByteData x = kvt.get(b("10"));
            Assert.assertEquals(x, b("dd"));
            kvt.put(b("10"), b("ee"));
            final ByteData y = kvt.get(b("10"));
            Assert.assertEquals(y, b("ee"));
        });
        this.log.info("testSimpleStuff() on {}: committed tx5", store);

        // Check read-only transaction (part 3)
        this.log.info("testSimpleStuff() on {}: starting tx6", store);
        this.tryNtimes(store, kvt -> {
            kvt.setReadOnly(true);
            final ByteData x = kvt.get(b("10"));
            Assert.assertEquals(x, b("dd"));
        });
        this.log.info("testSimpleStuff() on {}: committed tx6", store);
    }

    @Test(dataProvider = "kvdbs")
    public void testReadOnly(KVDatabase store) throws Exception {

        // Debug
        this.log.info("starting testReadOnly() on {}", store);

        // Put some data in database
        this.log.info("testReadOnly() on {}: initializing database", store);
        this.tryNtimes(store, tx -> {
            tx.removeRange(null, null);
            tx.put(ByteData.empty(), ByteData.empty());
        });
        this.log.info("testReadOnly() on {}: done initializing database", store);

        // Test we can set a transaction to read-only mode before reading some data
        this.log.info("testReadOnly() on {}: testing setReadOnly() before access", store);
        this.tryNtimes(store, tx -> {
            tx.setReadOnly(true);
            final ByteData value1 = tx.get(ByteData.empty());
            Assert.assertEquals(value1, ByteData.empty());
            Assert.assertTrue(tx.isReadOnly());
        });
        this.log.info("testReadOnly() on {}: done testing setReadOnly() after access", store);

        // Test we can set a transaction to read-only mode after reading some data
        if (this.supportsReadOnlyAfterDataAccess()) {
            this.log.info("testReadOnly() on {}: testing setReadOnly()", store);
            this.tryNtimes(store, tx -> {
                final ByteData value1 = tx.get(ByteData.empty());
                Assert.assertEquals(value1, ByteData.empty());
                tx.setReadOnly(true);
                final ByteData value2 = tx.get(ByteData.empty());
                Assert.assertEquals(value2, ByteData.empty());
                Assert.assertTrue(tx.isReadOnly());
            });
            this.log.info("testReadOnly() on {}: done testing setReadOnly()", store);
        }
    }

    private ByteData randomBytes(int index) {
        final byte[] array = new byte[this.random.nextInt(10) + 1];
        this.random.nextBytes(array);
        array[0] = (byte)index;
        return ByteData.of(array);
    }

    @Test(dataProvider = "kvdbs")
    public void testSortOrder(KVDatabase store) throws Exception {
        int index = 0;
        final ByteData[][] pairs = new ByteData[][] {
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
        this.log.info("starting testSortOrder() on {}", store);

        // Clear database
        this.log.info("testSortOrder() on {}: clearing database", store);
        this.tryNtimes(store, tx -> tx.removeRange(null, null));
        this.log.info("testSortOrder() on {}: done clearing database", store);

        // Write data
        this.log.info("testSortOrder() on {}: writing data", store);
        this.tryNtimes(store, tx -> {
            for (ByteData[] pair : pairs)
                tx.put(pair[0], pair[1]);
        });

        // Verify data
        this.log.info("testSortOrder() on {}: verifying data order", store);
        this.tryNtimes(store, tx -> {
            try (CloseableIterator<KVPair> i = tx.getRange(null, null, false)) {
                int index2 = 0;
                while (i.hasNext()) {
                    final ByteData[] pair = pairs[index2];
                    final KVPair kvpair = i.next();
                    Assert.assertTrue(kvpair.getKey().equals(pair[0]) && kvpair.getValue().equals(pair[1]),
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
        this.log.info("starting testKeyWatch() on {}", store);

        // Clear database
        this.log.info("testKeyWatch() on {}: clearing database", store);
        this.tryNtimes(store, tx -> tx.removeRange(null, null));
        this.log.info("testKeyWatch() on {}: done clearing database", store);

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
            this.log.info("testKeyWatch() on {}: creating key watch for {}", store, mod);
            final Future<Void> watch = this.tryNtimesWithResult(store, tx -> {
                try {
                    return tx.watchKey(b("0123"));
                } catch (UnsupportedOperationException e) {
                    return null;
                }
            });
            if (watch == null) {
                this.log.info("testKeyWatch() on {}: key watches not supported, bailing out", store);
                return;
            }
            this.log.info("testKeyWatch() on {}: created key watch: {}", store, watch);

            // Perform modification
            this.log.info("testKeyWatch() on {}: testing {}", store, mod);
            this.tryNtimes(store, mod);

            // Get notification
            this.log.info("testKeyWatch() on {}: waiting for notification", store);
            final long start = System.nanoTime();
            watch.get(1, TimeUnit.SECONDS);
            this.log.info("testKeyWatch() on {}: got notification in {}ms", store, (System.nanoTime() - start) / 1000000);
        }

        // Done
        this.log.info("finished testKeyWatch() on {}", store);
    }

    @Test(dataProvider = "kvdbs")
    public void testReadWriteConflict(KVDatabase store) throws Exception {
        this.testConflictingTransactions(store, "testReadWriteConflict", (tx1, tx2) -> {

            // Both read the same key
            this.threads[0].submit(new Reader(tx1, b("10"))).get();
            this.threads[1].submit(new Reader(tx2, b("10"))).get();

            // Both write to the same key but with different values
            return new Future<?>[] {
              this.threads[0].submit(new Writer(tx1, b("10"), b("01"))),
              this.threads[1].submit(new Writer(tx2, b("10"), b("02")))
            };
        }, kv("10", "01"), kv("10", "02"));
    }

    // https://en.wikipedia.org/wiki/Snapshot_isolation
    @Test(dataProvider = "kvdbs")
    public void testWriteSkewAnomaly(KVDatabase store) throws Exception {
        if (this.allowsWriteSkewAnomaly()) {
            this.log.info("skipping testWriteSkewAnomaly() on {}: database allows write skew anomalies", store);
            return;
        }
        this.testConflictingTransactions(store, "testWriteSkewAnomaly", (tx1, tx2) -> {

            // Both read both keys
            this.threads[0].submit(new Reader(tx1, b("10"))).get();
            this.threads[1].submit(new Reader(tx2, b("10"))).get();
            this.threads[0].submit(new Reader(tx1, b("20"))).get();
            this.threads[1].submit(new Reader(tx2, b("20"))).get();

            // Each writes to one of the keys
            return new Future<?>[] {
              this.threads[0].submit(new Writer(tx1, b("10"), b("01"))),
              this.threads[1].submit(new Writer(tx2, b("20"), b("02")))
            };
        }, kv("10", "01"), kv("20", "02"));
    }

    protected void testConflictingTransactions(KVDatabase store, String name,
      Conflictor conflictor, KVPair expected1, KVPair expected2) throws Exception {

        // Conflicts handled?
        if (!this.supportsMultipleWriteTransactions()) {
            this.log.info("skipping {}() on {}: database doesn't support simultaneous writers", name, store);
            return;
        }

        // Clear database
        this.log.info("starting {}() on {}", name, store);
        this.tryNtimes(store, tx -> tx.removeRange(null, null));

        // Create two transactions
        final KVTransaction[] txs = new KVTransaction[] {
            this.threads[0].submit(() -> this.createKVTransaction(store)).get(),
            this.threads[1].submit(() -> this.createKVTransaction(store)).get()
        };
        this.log.info("tx[0] is {}", txs[0]);
        this.log.info("tx[1] is {}", txs[1]);

        // Do the conflicting activity
        Future<?>[] futures = conflictor.conflict(txs[0], txs[1]);

        // See what happened - we might have gotten a conflict at write time
        final String[] fails = new String[] { "uninitialized status", "uninitialized status" };
        for (int i = 0; i < 2; i++) {
            try {
                futures[i].get();
                this.log.info("{} #{} succeeded on write", txs[i], i + 1);
                fails[i] = null;
            } catch (Throwable e) {
                while (e instanceof ExecutionException)
                    e = e.getCause();
                if (e instanceof AssertionError)
                    throw (AssertionError)e;
                if (e.toString().contains(AssertionError.class.getName()))
                    throw new AssertionError("internal assertion failure", e);
                if (!(e instanceof RetryKVTransactionException))
                    throw new AssertionError("wrong exception type: " + e, e);
                final RetryKVTransactionException retry = (RetryKVTransactionException)e;
                //Assert.assertSame(retry.getTransaction(), txs[i]);
                this.log.info("{} #{} failed on write", txs[i], i + 1);
                if (this.log.isTraceEnabled())
                    this.log.info("{} #{} write failure exception trace", txs[i], i + 1, e);
                fails[i] = "" + e;
            }
        }

        // Show contents of surviving transactions; note exception(s) could occur here also
        for (int i = 0; i < 2; i++) {
            if (fails[i] == null) {
                final RuntimeException e = this.showKV(txs[i], String.format("tx[%d] of %s after write", i, store));
                if (e != null) {
                    if (e.toString().contains(AssertionError.class.getName()))
                        throw new AssertionError("internal assertion failure", e);
                    fails[i] = "" + e;
                }
            }
        }

        // If both succeeded, then we should get a conflict on commit instead
        for (int i = 0; i < 2; i++) {
            if (fails[i] == null)
                futures[i] = this.threads[i].submit(new Committer(txs[i]));
        }
        for (int i = 0; i < 2; i++) {
            if (fails[i] == null) {
                try {
                    futures[i].get();
                    this.log.info("{} #{} succeeded on commit", txs[i], i + 1);
                    fails[i] = null;
                } catch (AssertionError e) {
                    throw e;
                } catch (Throwable e) {
                    while (e instanceof ExecutionException)
                        e = e.getCause();
                    if (e instanceof AssertionError)
                        throw (AssertionError)e;
                    if (e.toString().contains(AssertionError.class.getName()))
                        throw new AssertionError("internal assertion failure", e);
                    assert e instanceof RetryKVTransactionException : String.format("wrong exception type: %s", e);
                    final RetryKVTransactionException retry = (RetryKVTransactionException)e;
                    //Assert.assertSame(retry.getTransaction(), txs[i]);
                    this.log.info("{} #{} failed on commit", txs[i], i + 1);
                    if (this.log.isTraceEnabled())
                        this.log.trace("{} #{} commit failure exception trace", txs[i], i + 1, e);
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
        this.log.info("exactly one transaction failed:\n  fails[0]: {}\n  fails[1]: {}", fails[0], fails[1]);

        // Verify the resulting change is consistent with the tx that succeeded
        final KVPair expected = fails[0] == null ? expected1 : fails[1] == null ? expected2 : null;
        if (expected != null) {
            final KVTransaction tx2 = this.threads[0].submit(() -> this.createKVTransaction(store)).get();
            this.showKV(tx2, "TX2 of " + store);
            ByteData val = this.threads[0].submit(new Reader(tx2, expected.getKey())).get();
            Assert.assertEquals(val, expected.getValue());
            tx2.rollback();
        }
        this.log.info("finished {}() on {}", name, store);
    }

    @FunctionalInterface
    private interface Conflictor {
        Future<?>[] conflict(KVTransaction tx1, KVTransaction tx2) throws Exception;
    }

    @Test(dataProvider = "kvdbs")
    public void testNonconflictingTransactions(KVDatabase store) throws Exception {
        if (!this.supportsMultipleWriteTransactions()) {
            this.log.info("skipping testNonconflictingTransactions() on {}: database doesn't support simultaneous writers", store);
            return;
        }

        // Clear database
        this.log.info("starting testNonconflictingTransactions() on {}", store);
        this.tryNtimes(store, tx -> tx.removeRange(null, null));

        // Multiple concurrent transactions with overlapping read ranges and non-intersecting write ranges
        int done = 0;
        final KVTransaction[] txs = new KVTransaction[this.getNonconflictingTransactionCount()];
        for (int i = 0; i < txs.length; i++)
            txs[i] = this.threads[i].submit(() -> this.createKVTransaction(store)).get();
        while (true) {
            boolean finished = true;
            for (int i = 0; i < txs.length; i++) {
                if (txs[i] == null)
                    continue;
                finished = false;
                final Future<?> rf = this.threads[i].submit(new Reader(txs[i], ByteData.of((byte)i), true));
                final Future<?> wf = this.threads[i].submit(new Writer(txs[i], ByteData.of((byte)(i + 128)), b("02")));
                for (Future<?> f : new Future<?>[] { rf, wf }) {
                    try {
                        f.get();
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof RetryKVTransactionException) {
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
                    try {
                        final KVTransaction tx = txs[i];
                        this.threads[i].submit(() -> tx.commit()).get();
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof Error)
                            throw (Error)e.getCause();
                        if (e.getCause() instanceof RuntimeException)
                            throw (RuntimeException)e.getCause();
                        throw new RuntimeException(e.getCause());
                    }
                } catch (RetryKVTransactionException e) {
                    this.updateRetryStats(e);
                    txs[i] = this.threads[i].submit(() -> this.createKVTransaction(store)).get();
                    continue;
                }
                txs[i] = null;
            }
        }
        this.log.info("finished testNonconflictingTransactions() on {}", store);
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
        if (!this.supportsMultipleWriteTransactions()) {
            this.log.info("skipping testParallelTransactions() on {}: database doesn't support simultaneous writers", store);
            return;
        }
        this.testParallelTransactions(new KVDatabase[] { store });
    }

    public void testParallelTransactions(KVDatabase[] stores) throws Exception {
        this.log.info("starting testParallelTransactions() on {}", Arrays.asList(stores));
        for (int count = 0; count < this.getParallelTransactionLoopCount(); count++) {
            this.log.info("starting testParallelTransactions() iteration {}", count);
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
                    throw new Exception(String.format("task #%d failed: >>>%s<<<", i, this.show(fail).trim()));
            }
            this.log.info("finished testParallelTransactions() iteration {}", count);
        }
        this.log.info("finished testParallelTransactions() on {}", Arrays.asList(stores));
        for (KVDatabase store : stores) {
            if (store instanceof Closeable)
                ((Closeable)store).close();
        }
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
        this.log.info("starting testSequentialTransactions() on {}", store);

        // Clear database
        this.tryNtimes(store, tx -> tx.removeRange(null, null));

        // Keep an in-memory record of what is in the committed database
        final TreeMap<ByteData, ByteData> committedData = new TreeMap<>();

        // Run transactions
        for (int i = 0; i < this.getSequentialTransactionLoopCount(); i++) {
            final RandomTask task = new RandomTask(i, store, committedData, this.random.nextLong());
            task.run();
            final Throwable fail = task.getFail();
            if (fail != null)
                throw new Exception(String.format("task #%d failed: >>>%s<<<", i, this.show(fail).trim()));
        }
        this.log.info("finished testSequentialTransactions() on {}", store);
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
        if (!this.transactionsAreThreadSafe()) {
            this.log.info("skipping testMultipleThreadsTransaction() on {}: transactions are not thread safe", store);
            return;
        }
        this.log.info("starting testMultipleThreadsTransaction() on {}", store);

        // Clear database
        this.tryNtimes(store, tx -> tx.removeRange(null, null));

        // Populate database with some data
        final TreeMap<ByteData, ByteData> committedData = new TreeMap<>();
        final RandomTask populateTask = new RandomTask(-1, store, committedData, this.random.nextLong());
        this.log.info("testMultipleThreadsTransaction() populating database with random data");
        populateTask.run();
        Throwable fail = populateTask.getFail();
        if (fail != null)
            throw new Exception(String.format("populate task failed: >>>%s<<<", this.show(fail).trim()));

        // Create new transaction and blast away at it
        this.log.info("testMultipleThreadsTransaction() starting threads");
        this.tryNtimes(store, tx -> {

            // Create worker threads
            final RandomTask[] tasks = new RandomTask[33];
            final Thread[] workerThreads = new Thread[tasks.length];
            for (int i = 0; i < tasks.length; i++) {
                final RandomTask task = new RandomTask(i, null, KVDatabaseTest.this.random.nextLong());
                workerThreads[i] = new Thread(() -> task.runRandomAccess(tx));
                tasks[i] = task;
            }

            // Start worker threads
            for (int i = 0; i < tasks.length; i++)
                workerThreads[i].start();

            // Join worker threads
            for (int i = 0; i < tasks.length; i++) {
                try {
                    workerThreads[i].join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            // Check for errors
            for (int i = 0; i < tasks.length; i++) {
                final Throwable taskFail = tasks[i].getFail();
                if (taskFail != null) {
                    throw new RuntimeException(String.format(
                      "task #%d failed: >>>%s<<<", i, KVDatabaseTest.this.show(taskFail).trim()));
                }
            }
        });
        this.log.info("finished testMultipleThreadsTransaction() on {}", store);
    }

    /**
     * Test KVStore.apply().
     *
     * @param store database
     * @throws Exception if an error occurs
     */
    @Test(dataProvider = "kvdbs")
    public void testApplyMutations(KVDatabase store) throws Exception {
        this.log.info("starting testApplyMutations() on {}", store);

        // Create some mutations
        final RandomTask task = new RandomTask(0, store, this.random.nextLong());
        final MutableView mutableView = new MutableView(new MemoryKVStore());
        final Mutations mutations = mutableView.getWrites();
        task.performRandomAccess(mutableView);

        // Clear database
        this.tryNtimes(store, tx -> tx.removeRange(null, null));

        // Apply them using remove(), removeRange(), put(), and adjustCounter()
        this.tryNtimes(store, tx -> {
            try (Stream<KeyRange> removes = mutations.getRemoveRanges()) {
                removes.iterator().forEachRemaining(remove -> {
                    final ByteData min = remove.getMin();
                    final ByteData max = remove.getMax();
                    assert min != null;
                    if (max != null && ByteUtil.isConsecutive(min, max))
                        tx.remove(min);
                    else
                        tx.removeRange(min, max);
                });
            }
            try (Stream<Map.Entry<ByteData, ByteData>> puts = mutations.getPutPairs()) {
                puts.iterator().forEachRemaining(entry -> tx.put(entry.getKey(), entry.getValue()));
            }
            try (Stream<Map.Entry<ByteData, Long>> adjusts = mutations.getAdjustPairs()) {
                adjusts.iterator().forEachRemaining(entry -> tx.adjustCounter(entry.getKey(), entry.getValue()));
            }
        });
        final TreeMap<ByteData, ByteData> expected = task.readDatabase();

        // Clear database
        this.tryNtimes(store, tx -> tx.removeRange(null, null));

        // Apply them using apply()
        this.tryNtimes(store, tx -> tx.apply(mutations));
        final TreeMap<ByteData, ByteData> actual = task.readDatabase();

        // Verify equal
        Assert.assertEquals(stringView(actual), stringView(expected), "apply() failed:"
          + "\n  mutations=" + mutations
          + "\n  expected=" + stringView(expected)
          + "\n  actual=" + stringView(actual));
        this.log.info("finished testApplyMutations() on {}", store);
    }

// RandomTask

    public class RandomTask extends Thread {

        private final int id;
        private final KVDatabase store;
        private final Random random;
        private final boolean randomSleeps;
        private final TreeMap<ByteData, ByteData> committedData;            // tracks actual committed data, if known
        private final NavigableMap<String, String> committedDataView;
        private final ArrayList<String> log = new ArrayList<>(KVDatabaseTest.this.getRandomTaskMaxIterations());

        private Throwable fail;

        public RandomTask(int id, KVDatabase store, long seed) {
            this(id, store, null, seed);
        }

        public RandomTask(int id, KVDatabase store, TreeMap<ByteData, ByteData> committedData, long seed) {
            super("Random[" + id + "," + (committedData != null ? "SEQ" : "PAR") + "]");
            this.id = id;
            this.store = store;
            this.committedData = committedData;
            this.committedDataView = stringView(this.committedData);
            this.random = new Random(seed);
            this.randomSleeps = committedData == null;       // only add random sleeps in parallel mode
            this.log("seed = {}", seed);
        }

        @Override
        public void run() {
            KVDatabaseTest.this.log.debug("*** {} STARTING", this);
            try {
                this.test();
                this.log("succeeded");
            } catch (Throwable t) {
                final StringWriter buf = new StringWriter();
                t.printStackTrace(new PrintWriter(buf, true));
                this.log("failed: {}\n{}", t, buf);
                this.fail = t;
            } finally {
                KVDatabaseTest.this.log.debug("*** {} FINISHED", this);
                this.dumpLog(this.fail != null);
            }
        }

        public Throwable getFail() {
            return this.fail;
        }

        @SuppressWarnings("unchecked")
        private void test() throws Exception {

            // Keep track of key/value pairs that we know should exist in the transaction
            final TreeMap<ByteData, ByteData> knownValues = new TreeMap<>();
            final NavigableMap<String, String> knownValuesView = stringView(knownValues);
            final TreeSet<ByteData> putValues = new TreeSet<>();
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
            KVDatabaseTest.this.log.debug("*** CREATED {} TX {}", readOnly ? "R/O" : "R/W", tx);
            assert !tx.isReadOnly();
            if (readOnly) {
                tx.setReadOnly(true);
                assert tx.isReadOnly();
            }

            // Save a copy of committed data
            final TreeMap<ByteData, ByteData> previousCommittedData = this.committedData != null ?
              (TreeMap<ByteData, ByteData>)this.committedData.clone() : null;
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
                    ByteData key;
                    ByteData val;
                    ByteData min;
                    ByteData max;
                    KVPair pair;
                    int option = this.r(62);
                    boolean knownValuesChanged = false;
                    if (option < 10) {                                              // get
                        key = this.rb(1, false);
                        val = tx.get(key);
                        this.log("get: {} -> {}", s(key), s(val));
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
                        this.log("put: {} -> {}", s(key), s(val));
                        tx.put(key, val);
                        knownValues.put(key, val);
                        putValues.add(key);
                        knownEmpty.remove(new KeyRange(key));
                        knownValuesChanged = true;
                    } else if (option < 30) {                                       // getAtLeast
                        min = this.rb(1, true);
                        do {
                            max = this.rb2(this.r(2) + 1, 20);
                        } while (max != null && min != null && min.compareTo(max) > 0);
                        pair = tx.getAtLeast(min, max);
                        this.log("getAtLeast: {},{} -> {}", s(min), s(max), s(pair));
                        assert pair == null || pair.getKey().compareTo(min) >= 0;
                        assert pair == null || max == null || pair.getKey().compareTo(max) < 0;
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
                        } while (max != null && min != null && min.compareTo(max) > 0);
                        pair = tx.getAtMost(max, min);
                        this.log("getAtMost: {},{} -> {}", s(max), s(min), s(pair));
                        assert pair == null || min == null || pair.getKey().compareTo(min) >= 0;
                        assert pair == null || pair.getKey().compareTo(max) < 0;
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
                          pair != null ? ByteUtil.getNextKey(pair.getKey()) : min != null ? min : ByteData.empty(),
                          max));
                        knownValuesChanged = true;
                    } else if (option < 50) {                                       // remove
                        key = this.rb(1, false);
                        if (this.r(5) == 0 && (pair = tx.getAtLeast(this.rb(1, false), null)) != null)
                            key = pair.getKey();
                        this.log("remove: {}", s(key));
                        tx.remove(key);
                        knownValues.remove(key);
                        putValues.remove(key);
                        knownEmpty.add(new KeyRange(key));
                        knownValuesChanged = true;
                    } else if (option < 52) {                                       // removeRange
                        min = this.rb2(2, 20);
                        do {
                            max = this.rb2(2, 30);
                        } while (max != null && min != null && min.compareTo(max) > 0);
                        this.log("removeRange: {} to {}", s(min), s(max));
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
                        knownEmpty.add(new KeyRange(min != null ? min : ByteData.empty(), max));
                        knownValuesChanged = true;
                    } else if (option < 60) {                                       // adjustCounter
                        key = this.rb(1, false);
                        key = ByteData.of((byte)(key.byteAt(0) & 0x0f));
                        val = tx.get(key);
                        long counter = -1;
                        if (val != null) {
                            try {
                                counter = tx.decodeCounter(val);
                                this.log("adj: found valid value {} ({}) at key {}", s(val), counter, s(key));
                            } catch (IllegalArgumentException e) {
                                this.log("adj: found bogus value {} key {}", s(val), s(key));
                                val = null;
                            }
                        }
                        if (val == null) {
                            counter = this.random.nextLong();
                            final ByteData encodedCounter = tx.encodeCounter(counter);
                            tx.put(key, encodedCounter);
                            putValues.add(key);
                            this.log("adj: initialize {} to {}", s(key), s(encodedCounter));
                        }
                        final long adj = this.random.nextInt(1 << this.random.nextInt(24)) - 1024;
                        final ByteData encodedCounter = tx.encodeCounter(counter + adj);
                        this.log("adj: {} by {} -> should now be {}", s(key), adj, s(encodedCounter));
                        tx.adjustCounter(key, adj);
                        knownValues.put(key, encodedCounter);
                        knownEmpty.remove(new KeyRange(key));
                        knownValuesChanged = true;
                    } else if (this.randomSleeps) {                                 // sleep
                        final int millis = this.r(50);
                        this.log("sleep {}ms", millis);
                        try {
                            Thread.sleep(millis);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                    if (knownValuesChanged) {
                        this.log("new values:"
                          + "\n  knowns={}"
                          + "\n  puts={}"
                          + "\n  emptys={}",
                          knownValuesView,
                          putValuesView,
                          knownEmpty);
                    }

                    // Verify everything we know to be there is there
                    for (Map.Entry<ByteData, ByteData> entry : knownValues.entrySet()) {
                        final ByteData knownKey = entry.getKey();
                        final ByteData expected = entry.getValue();
                        final ByteData actual = tx.get(knownKey);
                        assert actual != null && actual.compareTo(expected) == 0 :
                          this + ": tx has " + s(actual) + " for key " + s(knownKey) + " but"
                          + "\n  knowns=" + knownValuesView + "\n  puts=" + putValuesView
                          + "\n  emptys=" + knownEmpty + "\n  tx=" + this.toString(tx);
                    }

                    // Verify everything we know to no be there is not there
                    try (CloseableIterator<KVPair> iter = tx.getRange(null, null, false)) {
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
                KVDatabaseTest.this.log.debug("*** {} {} TX {}",
                  rollback ? "ROLLING BACK" : "COMMITTING", readOnly ? "R/O" : "R/W", tx);
                this.log("about to {}:"
                  + "\n  knowns={}"
                  + "\n  puts={}"
                  + "\n  emptys={}"
                  + "\n  committed: {}",
                  rollback ? "rollback" : "commit",
                  knownValuesView,
                  putValuesView,
                  knownEmpty,
                  committedDataView);
                if (rollback) {
                    tx.rollback();
                    committed = false;
                    this.log("rolled-back");
                } else {
                    try {
                        KVDatabaseTest.this.numTransactionAttempts.incrementAndGet();
                        tx.commit();
                    } catch (RetryKVTransactionException e) {
                        KVDatabaseTest.this.updateRetryStats(e);
                        throw e;
                    }
                    committed = true;
                    KVDatabaseTest.this.log.debug("*** COMMITTED TX {}", tx);
                    this.log("committed");
                }

            } catch (KVTransactionTimeoutException e) {
                KVDatabaseTest.this.log.debug("*** TX {} THREW {}", tx, e.toString());
                this.log("got {}", e.toString());
                committed = false;
            } catch (RetryKVTransactionException e) {             // might have committed, might not have, we don't know for sure
                KVDatabaseTest.this.log.debug("*** TX {} THREW {}", tx, e.toString());
                this.log("got {}", e.toString());
            }

            // Doing this should always be allowed and shouldn't affect anything
            tx.rollback();

            // Verify committed database contents are now equal to what's expected
            if (this.committedData != null) {

                // Read actual content
                final TreeMap<ByteData, ByteData> actual = new TreeMap<>();
                final NavigableMap<String, String> actualView = stringView(actual);
                actual.putAll(this.readDatabase());

                // Update what we think is in the database and then compare to actual content
                if (Boolean.TRUE.equals(committed) && !readOnly) {

                    // Verify
                    this.log("tx was definitely committed");
                    assert actualView.equals(knownValuesView) :
                      this + "\n*** ACTUAL:\n" + actualView + "\n*** EXPECTED:\n" + knownValuesView + "\n";

                    // Update model of database
                    this.committedData.clear();
                    this.committedData.putAll(knownValues);
                } else if (Boolean.FALSE.equals(committed) || readOnly) {

                    // Verify
                    this.log("tx was definitely rolled back (committed={}, readOnly={})", committed, readOnly);
                    assert actualView.equals(committedDataView) :
                      this + "\n*** ACTUAL:\n" + actualView + "\n*** EXPECTED:\n" + committedDataView + "\n";
                    committed = false;
                } else {

                    // We don't know whether transaction got committed or not. In fact, a mutating commit may appear
                    // to have taken effect after the "actual" snapshot because the transaction could have terminated early.
                    // To get a reliable update of the actual database contents, we must commit a mutating transaction.
                    this.committedData.clear();
                    this.committedData.putAll(KVDatabaseTest.this.tryNtimesWithResult(this.store, kvt -> {

                        // Make a definite mutation
                        KVPair pair = kvt.getAtLeast(null, null);
                        ByteData key;
                        ByteData val;
                        if (pair != null) {
                            key = pair.getKey();
                            val = pair.getValue();
                            if (val.isEmpty())
                                val = this.rb(2, false);
                            else
                                val = ByteData.of((byte)(val.byteAt(0) + 77)).concat(val.substring(1));
                        } else {
                            key = this.rb(1, false);
                            val = this.rb(2, false);
                        }
                        kvt.put(key, val);

                        // Re-read database contents
                        return this.readDatabase(kvt);
                    }));
                }
            }
        }

        public void runRandomAccess(KVStore tx) {
            KVDatabaseTest.this.log.debug("*** {} runRandomAccess() STARTING", this);
            try {
                this.performRandomAccess(tx);
            } catch (Throwable t) {
                this.fail = t;
            } finally {
                KVDatabaseTest.this.log.debug("*** {} runRandomAccess() FINISHED", this);
            }
        }

        public void performRandomAccess(KVStore tx) {
            final int limit = this.r(KVDatabaseTest.this.getRandomTaskMaxIterations());
            for (int j = 0; j < limit; j++) {
                ByteData key;
                ByteData val;
                ByteData min;
                ByteData max;
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
                    } while (max != null && min != null && min.compareTo(max) > 0);
                    tx.getAtLeast(min, max);
                } else if (option < 40) {                                       // getAtMost
                    max = this.rb(1, true);
                    do {
                        min = this.rb2(this.r(2) + 1, 20);
                    } while (max != null && min != null && min.compareTo(max) > 0);
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
                    } while (max != null && min != null && min.compareTo(max) > 0);
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

        public TreeMap<ByteData, ByteData> readDatabase() {
            return KVDatabaseTest.this.tryNtimesWithResult(this.store, RandomTask.this::readDatabase);
        }

        private TreeMap<ByteData, ByteData> readDatabase(KVStore tx) {
            final TreeMap<ByteData, ByteData> values = new TreeMap<>();
            try (CloseableIterator<KVPair> i = tx.getRange(null, null, false)) {
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
            try (CloseableIterator<KVPair> i = kv.getRange(null, null, false)) {
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

        private void log(String s, Object... args) {
            final int numArgs = args.length;
            if (args.length > 0) {
                final String[] parts = s.split("\\{\\}", numArgs + 1);
                final StringBuilder buf = new StringBuilder();
                for (int i = 0; i < parts.length - 1; i++)
                    buf.append(parts[i]).append(args[i]);
                buf.append(parts[parts.length - 1]);
                s = buf.toString();
            }
            this.log.add(s);
        }

        private void dumpLog(boolean force) {
            if (!force && !KVDatabaseTest.this.log.isTraceEnabled())
                return;
            synchronized (KVDatabaseTest.this) {
                final StringBuilder buf = new StringBuilder(this.log.size() * 40);
                for (String s : this.log)
                    buf.append(s).append('\n');
                KVDatabaseTest.this.log.debug("*** BEGIN {} LOG ***\n\n{}\n*** END {} LOG ***", this, buf, this);
            }
        }

        private int r(int max) {
            return this.random.nextInt(max);
        }

        private ByteData rb(int len, boolean allowFF) {
            final byte[] b = new byte[this.r(len) + 1];
            this.random.nextBytes(b);
            if (!allowFF && b[0] == (byte)0xff)
                b[0] = (byte)random.nextInt(0xff);
            return ByteData.of(b);
        }

        private ByteData rb2(int len, int nullchance) {
            if (this.r(nullchance) == 0)
                return null;
            return this.rb(len, true);
        }

        @Override
        public String toString() {
            return "Random[" + this.id + "," + (this.committedData != null ? "SEQ" : "PAR") + "]";
        }
    }

// Reader

    public class Reader implements Callable<ByteData> {

        final KVTransaction tx;
        final ByteData key;
        final boolean range;

        public Reader(KVTransaction tx, ByteData key, boolean range) {
            this.tx = tx;
            this.key = key;
            this.range = range;
        }

        public Reader(KVTransaction tx, ByteData key) {
            this(tx, key, false);
        }

        @Override
        public ByteData call() {
            if (this.range) {
                if (KVDatabaseTest.this.log.isTraceEnabled())
                    KVDatabaseTest.this.log.trace("reading at least {} in {}", s(this.key), this.tx);
                final KVPair pair = this.tx.getAtLeast(this.key, null);
                KVDatabaseTest.this.log.info("finished reading at least {} -> {} in {}", s(this.key), pair, this.tx);
                return pair != null ? pair.getValue() : null;
            } else {
                if (KVDatabaseTest.this.log.isTraceEnabled())
                    KVDatabaseTest.this.log.trace("reading {} in {}", s(this.key), this.tx);
                final ByteData value = this.tx.get(this.key);
                KVDatabaseTest.this.log.info("finished reading {} -> {} in {}", s(this.key), s(value), this.tx);
                return value;
            }
        }
    }

// Writer

    public class Writer implements Runnable {

        final KVTransaction tx;
        final ByteData key;
        final ByteData value;

        public Writer(KVTransaction tx, ByteData key, ByteData value) {
            this.tx = tx;
            this.key = key;
            this.value = value;
        }

        @Override
        public void run() {
            try {
                KVDatabaseTest.this.log.info("putting {} -> {} in {}", s(this.key), s(this.value), this.tx);
                this.tx.put(this.key, this.value);
            } catch (RuntimeException e) {
                KVDatabaseTest.this.log.info("exception putting {} -> {} in {}: {}",
                  s(this.key), s(this.value), this.tx, e.toString());
                if (KVDatabaseTest.this.log.isTraceEnabled()) {
                    KVDatabaseTest.this.log.trace("{} put {} -> {} failure exception trace",
                      this.tx, s(this.key), s(this.value), e);
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
                KVDatabaseTest.this.log.info("committing {}", this.tx);
                KVDatabaseTest.this.numTransactionAttempts.incrementAndGet();
                this.tx.commit();
            } catch (RuntimeException e) {
                if (e instanceof RetryKVTransactionException)
                    KVDatabaseTest.this.updateRetryStats((RetryKVTransactionException)e);
                KVDatabaseTest.this.log.info("exception committing {}: {}", this.tx, e.toString());
                if (KVDatabaseTest.this.log.isTraceEnabled())
                    KVDatabaseTest.this.log.trace("{} commit failure exception trace", this.tx, e);
                throw e;
            }
        }
    }
}
