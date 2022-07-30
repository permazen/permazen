
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.dellroad.stuff.net.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link Network} that can introduce errors and delays.
 */
public class TestNetwork implements Network {

    private static final HashMap<String, TestNetwork> ALL_NETWORKS = new HashMap<>();

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String identity;
    private final int delayAverage;
    private final int delayStdDev;
    private final float failureRate;
    private final Random random = new Random();

    private ScheduledExecutorService executor;
    private Handler handler;

    private volatile Throwable lastException;

    /**
     * Constructor.
     *
     * @param delayAverage average message delay in milliseconds
     * @param delayStdDev standard deviation of message delay in milliseconds
     * @param failureRate probability that a message will get dropped
     */
    public TestNetwork(String identity, int delayAverage, int delayStdDev, float failureRate) {
        Preconditions.checkArgument(delayAverage >= 0, "delayAverage < 0");
        Preconditions.checkArgument(delayStdDev >= 0, "delayStdDev < 0");
        this.identity = identity;
        this.delayAverage = delayAverage;
        this.delayStdDev = delayStdDev;
        this.failureRate = failureRate;
    }

    @Override
    public void start(Handler handler) throws IOException {
        Preconditions.checkArgument(handler != null, "null handler");
        synchronized (ALL_NETWORKS) {
            Preconditions.checkState(this.handler == null, "already started");
            Preconditions.checkState(!ALL_NETWORKS.containsKey(this.identity),
              "network identity \"" + this.identity + "\" already in use");
            ALL_NETWORKS.put(this.identity, this);
            this.handler = handler;
            this.executor = Executors.newSingleThreadScheduledExecutor();
        }
    }

    @Override
    public void stop() {
        synchronized (ALL_NETWORKS) {
            if (this.handler == null)
                return;
            this.handler = null;
            this.executor.shutdown();
            this.executor = null;
            ALL_NETWORKS.remove(this.identity);
        }
    }

    @Override
    public boolean send(final String peer, final ByteBuffer msg) {
        synchronized (ALL_NETWORKS) {

            // Sanity check
            Preconditions.checkState(this.handler != null, "not started");
            final TestNetwork target = ALL_NETWORKS.get(peer);
            if (target == null)
                return false;

            // Calculate random delay
            final int delay = Math.max(0, (int)(this.delayAverage + this.random.nextGaussian() * this.delayStdDev));

            // Notify caller that senders's output queue is empty after 10% of delay
            this.executor.schedule(new OutputEmptyTask(this, peer), (int)(delay * 0.10f), TimeUnit.MILLISECONDS);

            // Randomly drop packagte
            final boolean drop = this.random.nextFloat() < this.failureRate;
            if (drop)
                return true;

            // Send packet after delay
            target.executor.schedule(new DeliverTask(target, this.identity, msg.asReadOnlyBuffer()), delay, TimeUnit.MILLISECONDS);
        }
        return true;
    }

    public Throwable getLastException() {
        return this.lastException;
    }

// NetworkTask

    private abstract static class NetworkTask implements Runnable {

        protected final TestNetwork target;

        NetworkTask(TestNetwork target) {
            this.target = target;
        }

        @Override
        public final void run() {
            try {
                final Handler targetHandler;
                synchronized (ALL_NETWORKS) {
                    targetHandler = this.target.handler;
                }
                if (targetHandler != null)
                    this.performTask(targetHandler);
            } catch (Throwable t) {
                this.target.lastException = t;
                this.target.log.error("error in network callback (\"{}\")", this.target.identity, t);
            }
        }

        protected abstract void performTask(Handler handler);
    }

// DeliverTask

    private static class DeliverTask extends NetworkTask {

        private final String sender;
        private final ByteBuffer msg;

        DeliverTask(TestNetwork target, String sender, ByteBuffer msg) {
            super(target);
            this.sender = sender;
            this.msg = msg;
        }

        @Override
        protected void performTask(Handler handler) {
            handler.handle(this.sender, this.msg);
        }
    }

// OutputEmptyTask

    private static class OutputEmptyTask extends NetworkTask {

        private final String recipient;

        OutputEmptyTask(TestNetwork target, String recipient) {
            super(target);
            this.recipient = recipient;
        }

        @Override
        protected void performTask(Handler handler) {
            handler.outputQueueEmpty(this.recipient);
        }
    }
}
