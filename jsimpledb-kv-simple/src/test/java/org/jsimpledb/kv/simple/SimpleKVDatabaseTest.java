
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.simple;

import com.google.common.base.Converter;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.test.KVDatabaseTest;
import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class SimpleKVDatabaseTest extends KVDatabaseTest {

    protected long timeoutTestStartTime;

    private SimpleKVDatabase simpleKV;

    @BeforeClass(groups = "configure")
    @Parameters("testSimpleKV")
    public void setTestSimpleKV(@Optional String testSimpleKV) {
        if (testSimpleKV != null && Boolean.valueOf(testSimpleKV))
            this.simpleKV = new SimpleKVDatabase(new NavigableMapKVStore(), 250, 5000);
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.simpleKV;
    }

    @Test
    public void testSimpleKVTimeouts() throws Exception {

        // Test hold and wait timeouts both not attained
        this.timeoutTestStartTime = System.currentTimeMillis();
        SimpleKVDatabase store = new SimpleKVDatabase(200, 400);
        HolderThread holderThread = new HolderThread(store, 100);
        WaiterThread waiterThread = new WaiterThread(store, holderThread);
        holderThread.start();
        waiterThread.start();
        holderThread.join();
        waiterThread.join();
        Assert.assertEquals(holderThread.getResult(), "success");
        Assert.assertEquals(waiterThread.getResult(), "success");

        // Test wait but not hold timeout attained
        this.timeoutTestStartTime = System.currentTimeMillis();
        store = new SimpleKVDatabase(200, 400);
        holderThread = new HolderThread(store, 300);
        waiterThread = new WaiterThread(store, holderThread);
        holderThread.start();
        waiterThread.start();
        holderThread.join();
        waiterThread.join();
        Assert.assertEquals(holderThread.getResult(), "success");
        Assert.assertEquals(waiterThread.getResult(), "RetryTransactionException");

        // Test hold timeout by itself - no exception because nobody is waiting
        this.timeoutTestStartTime = System.currentTimeMillis();
        store = new SimpleKVDatabase(100, 100);
        holderThread = new HolderThread(store, 200);
        holderThread.start();
        holderThread.join();
        Assert.assertEquals(holderThread.getResult(), "success");

        // Test hold but not wait timeout attained - exception because somebody is waiting
        this.timeoutTestStartTime = System.currentTimeMillis();
        store = new SimpleKVDatabase(400, 200);
        holderThread = new HolderThread(store, 300);
        waiterThread = new WaiterThread(store, holderThread);
        holderThread.start();
        waiterThread.start();
        holderThread.join();
        waiterThread.join();
        Assert.assertEquals(holderThread.getResult(), "TransactionTimeoutException");
        Assert.assertEquals(waiterThread.getResult(), "success");
    }

// TestThread

    public abstract class TestThread extends Thread {

        protected final SimpleKVDatabase store;

        protected String result;

        protected TestThread(SimpleKVDatabase store) {
            this.store = store;
        }

        public String getResult() {
            return this.result;
        }

        @Override
        public final void run() {
            try {
                this.doTest();
                this.result = "success";
            } catch (Throwable t) {
                if (t instanceof Error)
                    SimpleKVDatabaseTest.this.log.error("error thrown by test", t);
                this.result = t.getClass().getSimpleName();
            } finally {
                this.log("result = " + this.result);
            }
        }

        protected abstract void doTest() throws Exception;

        protected void log(String message) {
//            final long offset = System.currentTimeMillis() - SimpleKVDatabaseTest.this.timeoutTestStartTime;
//            SimpleKVDatabaseTest.this.log.info(String.format("[%04d]: ", offset)
//              + this.getClass().getSimpleName() + ": " + message);
        }
    }

    public class HolderThread extends TestThread {

        protected final long delay;

        private boolean ready;

        public HolderThread(SimpleKVDatabase store, long delay) {
            super(store);
            this.delay = delay;
        }

        @Override
        public void doTest() throws Exception {
            this.log("creating transaction");
            final KVTransaction tx = this.store.createTransaction();
            this.log("-> put()");
            tx.put(new byte[0], new byte[0]);
            this.log("<- put()");
            synchronized (this) {
                this.ready = true;
                this.notifyAll();
            }
            this.log("-> sleep(" + this.delay + ")");
            Thread.sleep(this.delay);
            this.log("<- sleep(" + this.delay + ")");
            tx.commit();
        }

        public synchronized void waitUntilReady() throws InterruptedException {
            while (!this.ready)
                this.wait();
        }
    }

    public class WaiterThread extends TestThread {

        private final HolderThread holderThread;

        public WaiterThread(SimpleKVDatabase store, HolderThread holderThread) {
            super(store);
            this.holderThread = holderThread;
        }

        @Override
        public void doTest() throws Exception {
            this.log("waiting for holder");
            this.holderThread.waitUntilReady();
            this.log("creating transaction");
            final KVTransaction tx = this.store.createTransaction();
            this.log("-> get()");
            tx.get(new byte[0]);
            this.log("<- get()");
            tx.commit();
        }
    }
}

