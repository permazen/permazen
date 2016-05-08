
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
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

    private static final HashMap<String, TestNetwork> NETWORK = new HashMap<>();

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String identity;
    private final int delayStdDev;
    private final float failureRate;
    private final ArrayDeque<Map.Entry<String, ByteBuffer>> inputQueue = new ArrayDeque<>();
    private final Random random = new Random();

    private ScheduledExecutorService executor;
    private Handler handler;

    private volatile Throwable lastException;

    /**
     * Constructor.
     *
     * @param delayStdDev standard deviation of the delay for messages in milliseconds
     * @param failureRate probability that a message will get dropped
     */
    public TestNetwork(String identity, int delayStdDev, float failureRate) {
        Preconditions.checkArgument(delayStdDev >= 0, "delayStdDev < 0");
        this.identity = identity;
        this.delayStdDev = delayStdDev;
        this.failureRate = failureRate;
    }

    @Override
    public void start(Handler handler) throws IOException {
        Preconditions.checkArgument(handler != null, "null handler");
        synchronized (NETWORK) {
            Preconditions.checkState(this.handler == null, "already started");
            Preconditions.checkState(!NETWORK.containsKey(this.identity),
              "network identity \"" + this.identity + "\" already in use");
            NETWORK.put(this.identity, this);
            this.handler = handler;
            this.executor = Executors.newSingleThreadScheduledExecutor();
        }
    }

    @Override
    public void stop() {
        synchronized (NETWORK) {
            if (this.handler == null)
                return;
            this.handler = null;
            this.executor.shutdown();
            this.executor = null;
            this.inputQueue.clear();
            NETWORK.remove(this.identity);
        }
    }

    @Override
    public boolean send(final String peer, final ByteBuffer msg) {
        synchronized (NETWORK) {

            // Sanity check
            Preconditions.checkState(this.handler != null, "not started");
            if (this.handler == null)
                return false;
            final TestNetwork target = NETWORK.get(peer);
            if (target == null)
                return false;

            // Randomly drop
            final boolean drop = this.random.nextFloat() < this.failureRate;

            // Calculate delay
            final int delay = (int)(Math.abs(this.random.nextGaussian()) * this.delayStdDev);

            // Send packet
            final Map.Entry<String, ByteBuffer> entry = new AbstractMap.SimpleEntry<>(this.identity, msg.asReadOnlyBuffer());
            target.executor.schedule(target.new NetworkRunnable() {
                @Override
                protected void doHandle(Handler handler) {
                    if (!drop)
                        handler.handle(TestNetwork.this.identity, msg.asReadOnlyBuffer());
                }
            }, delay, TimeUnit.MILLISECONDS);

            // Notify caller when recipient's output queue goes empty
            this.executor.schedule(this.new NetworkRunnable() {
                @Override
                protected void doHandle(Handler handler) {
                    handler.outputQueueEmpty(peer);
                }
            }, (int)(delay * 0.25f), TimeUnit.MILLISECONDS);
        }
        return true;
    }

    public Throwable getLastException() {
        return this.lastException;
    }

    private abstract class NetworkRunnable implements Runnable {

        @Override
        public final void run() {
            try {
                final Handler myHandler;
                synchronized (NETWORK) {
                    myHandler = TestNetwork.this.handler;
                }
                if (myHandler != null)
                    this.doHandle(myHandler);
            } catch (Throwable t) {
                TestNetwork.this.lastException = t;
                TestNetwork.this.log.error("error in network callback (\"" + TestNetwork.this.identity + "\")", t);
            }
        }

        protected abstract void doHandle(Handler handler);
    }
}

