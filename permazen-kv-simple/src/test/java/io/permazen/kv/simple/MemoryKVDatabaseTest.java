
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.test.KVDatabaseTest;
import io.permazen.util.ByteData;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class MemoryKVDatabaseTest extends KVDatabaseTest {

    protected long timeoutTestStartTime;

    private MemoryKVDatabase memoryKV;

    @BeforeClass(groups = "configure")
    @Parameters("testMemoryKV")
    public void setTestMemoryKV(@Optional String testMemoryKV) {
        if (testMemoryKV != null && Boolean.valueOf(testMemoryKV))
            this.memoryKV = new MemoryKVDatabase(250, 5000);
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.memoryKV;
    }

    @Test
    public void testMemoryKVTimeouts() throws Exception {

        // Test hold and wait timeouts both not attained
        this.timeoutTestStartTime = System.currentTimeMillis();
        MemoryKVDatabase store = new MemoryKVDatabase(200, 400);
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
        store = new MemoryKVDatabase(200, 400);
        holderThread = new HolderThread(store, 300);
        waiterThread = new WaiterThread(store, holderThread);
        holderThread.start();
        waiterThread.start();
        holderThread.join();
        waiterThread.join();
        Assert.assertEquals(holderThread.getResult(), "success");
        Assert.assertEquals(waiterThread.getResult(), "RetryKVTransactionException");

        // Test hold timeout by itself - no exception because nobody is waiting
        this.timeoutTestStartTime = System.currentTimeMillis();
        store = new MemoryKVDatabase(100, 100);
        holderThread = new HolderThread(store, 200);
        holderThread.start();
        holderThread.join();
        Assert.assertEquals(holderThread.getResult(), "success");

        // Test hold but not wait timeout attained - exception because somebody is waiting
        this.timeoutTestStartTime = System.currentTimeMillis();
        store = new MemoryKVDatabase(400, 200);
        holderThread = new HolderThread(store, 300);
        waiterThread = new WaiterThread(store, holderThread);
        holderThread.start();
        waiterThread.start();
        holderThread.join();
        waiterThread.join();
        Assert.assertEquals(holderThread.getResult(), "KVTransactionTimeoutException");
        Assert.assertEquals(waiterThread.getResult(), "success");
    }

// TestThread

    public abstract class TestThread extends Thread {

        protected final MemoryKVDatabase store;

        protected String result;

        protected TestThread(MemoryKVDatabase store) {
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
                    MemoryKVDatabaseTest.this.log.error("error thrown by test", t);
                this.result = t.getClass().getSimpleName();
            } finally {
                this.log("result = " + this.result);
            }
        }

        protected abstract void doTest() throws Exception;

        protected void log(String message) {
//            final long offset = System.currentTimeMillis() - MemoryKVDatabaseTest.this.timeoutTestStartTime;
//            MemoryKVDatabaseTest.this.log.info(String.format("[%04d]: ", offset)
//              + this.getClass().getSimpleName() + ": " + message);
        }
    }

    public class HolderThread extends TestThread {

        protected final long delay;

        private boolean ready;

        public HolderThread(MemoryKVDatabase store, long delay) {
            super(store);
            this.delay = delay;
        }

        @Override
        public void doTest() throws Exception {
            this.log("creating transaction");
            final KVTransaction tx = this.store.createTransaction();
            this.log("-> put()");
            tx.put(ByteData.empty(), ByteData.empty());
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

        public WaiterThread(MemoryKVDatabase store, HolderThread holderThread) {
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
            tx.get(ByteData.empty());
            this.log("<- get()");
            tx.commit();
        }
    }
}
