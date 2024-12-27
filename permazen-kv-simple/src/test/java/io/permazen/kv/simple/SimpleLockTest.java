
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import io.permazen.kv.KVPair;
import io.permazen.kv.test.KVTestSupport;
import io.permazen.util.ByteData;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SimpleLockTest extends KVTestSupport {

    private volatile int step;
    private MemoryKVDatabase db;

    @Test
    private void testSimpleLock() throws Exception {

        // Setup databse
        this.db = new MemoryKVDatabase();
        SimpleKVTransaction tx = db.createTransaction();
        tx.put(ByteData.fromHex("01"), ByteData.fromHex("aaaa"));
        tx.put(ByteData.fromHex("0101"), ByteData.fromHex("bbbb"));
        tx.put(ByteData.fromHex("0102"), ByteData.fromHex("cccc"));
        tx.put(ByteData.fromHex("02"), ByteData.fromHex("dddd"));
        tx.commit();

        // Start threads
        final Thread1 t1 = new Thread1();
        final Thread2 t2 = new Thread2();
        this.step = 0;
        t1.start();
        t2.start();

        // Run test
        this.step++;
        this.waitForStep(5);
        t1.join();
        t2.join();

        // Check results
        Assert.assertEquals(t1.getResult(), "OK");
        Assert.assertEquals(t2.getResult(), "OK");
    }

    private void waitForStep(int requiredStep) throws InterruptedException {
        this.log.debug("{} waiting for {}", Thread.currentThread().getName(), requiredStep);
        while (this.step < requiredStep)
            Thread.sleep(50);
        this.log.debug("{} ready for {}", Thread.currentThread().getName(), requiredStep);
    }

/*
    STEPS IN THIS TEST:

    1.  t1 deletes range [01, 02)
        - This causes t1 to acquire a write lock
    2.  t2 does getAtLeast(01)
        - This causes t2 to sleep waiting to acquire a read lock
        - The bug is/was that t2 would read the k/v store *before* acquiring the lock
    3.  t1 commits
        - The k/v store is updated, deleting range [01, 02)
        - t2 wakes up from sleep and getAtLeast(01) returns
    4.  t2 invokes getAtLeast(01) again
        - t2 should see a result consistent with step #2
*/

    private abstract class TestThread extends Thread {

        protected String result;
        protected SimpleKVTransaction tx;

        protected TestThread(String name) {
            super(name);
        }

        public String getResult() {
            return this.result;
        }

        @Override
        public void run() {
            try {
                this.tx = SimpleLockTest.this.db.createTransaction();
                this.result = this.doTest();
            } catch (Throwable t) {
                this.result = t.toString();
                SimpleLockTest.this.step = 5;
            }
        }

        protected abstract String doTest() throws Exception;
    }

    private class Thread1 extends TestThread {

        Thread1() {
            super("Thread1");
        }

        @Override
        protected String doTest() throws Exception {

            // Delete range, which creates write lock
            SimpleLockTest.this.waitForStep(1);
            SimpleLockTest.this.log.debug("Thread1 performing step 1");
            this.tx.removeRange(ByteData.fromHex("01"), ByteData.fromHex("02"));
            SimpleLockTest.this.step++;

            // Commit transaction
            SimpleLockTest.this.waitForStep(3);
            SimpleLockTest.this.log.debug("Thread1 performing step 3");
            this.tx.commit();
            SimpleLockTest.this.step++;

            // Done
            return "OK";
        }
    }

    private class Thread2 extends TestThread {

        Thread2() {
            super("Thread2");
        }

        @Override
        protected String doTest() throws Exception {

            // Do first query
            SimpleLockTest.this.waitForStep(2);
            SimpleLockTest.this.log.debug("Thread2 performing step 2");
            SimpleLockTest.this.step++;
            KVPair pair1 = this.tx.getAtLeast(ByteData.fromHex("01"), null);

            // Do second query
            SimpleLockTest.this.waitForStep(4);
            SimpleLockTest.this.log.debug("Thread2 performing step 4");
            KVPair pair2 = this.tx.getAtLeast(ByteData.fromHex("01"), null);
            SimpleLockTest.this.step++;

            // They should return the same result!
            return pair1 == null && pair2 == null ? "OK" :
              pair1 != null && pair1.equals(pair2) ? "OK" :
              "FAIL: pair1=" + pair1 + " but pair2=" + pair2;
        }
    }
}
