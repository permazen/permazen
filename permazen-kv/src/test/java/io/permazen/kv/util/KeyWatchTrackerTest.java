
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import io.permazen.test.TestSupport;
import io.permazen.util.ByteData;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyWatchTrackerTest extends TestSupport {

    private static final ByteData B1 = ByteData.of(12);
    private static final ByteData B2 = ByteData.of(34);
    private static final ByteData B3 = ByteData.of(56);

    @Test
    private void testTrigger1() throws Exception {
        final KeyWatchTracker tracker = new KeyWatchTracker();

        final ListenableFuture<?> f1 = tracker.register(B1);
        final AtomicBoolean flag = new AtomicBoolean();
        f1.addListener(() -> flag.set(true), MoreExecutors.directExecutor());

        tracker.trigger(B2);

        // Should still block
        this.verifyNotComplete(f1);

        tracker.trigger(B1);

        // Now should have an immediate trigger
        this.verifyComplete(f1);
        Thread.sleep(100);
        Assert.assertTrue(flag.get());

        // Done
        tracker.close();
    }

    @Test
    private void testTrigger2() throws Exception {
        final KeyWatchTracker tracker = new KeyWatchTracker();

        final ListenableFuture<?>[] futures1 = new ListenableFuture<?>[100];
        for (int i = 0; i < futures1.length; i++)
            futures1[i] = tracker.register(B1);

        final ListenableFuture<?>[] futures2 = new ListenableFuture<?>[100];
        for (int i = 0; i < futures2.length; i++)
            futures2[i] = tracker.register(B2);

        tracker.trigger(B1);

        for (ListenableFuture<?> future : futures1)
            this.verifyComplete(future);

        for (ListenableFuture<?> future : futures2)
            this.verifyNotComplete(future);
    }

    @Test
    private void testCapacity() throws Exception {
        final KeyWatchTracker tracker = new KeyWatchTracker(2, 99999, false);
        final ListenableFuture<?> f1 = tracker.register(B1);
        final ListenableFuture<?> f2 = tracker.register(B2);
        final ListenableFuture<?> f3 = tracker.register(B3);

        // This should have an immediate spurious trigger
        this.verifyComplete(f1);

        // These should still block
        this.verifyNotComplete(f2);
        this.verifyNotComplete(f3);
    }

    @Test
    private void testMaxLifetime() throws Exception {
        final KeyWatchTracker tracker = new KeyWatchTracker(99999, 1, false);
        final ListenableFuture<?> f1 = tracker.register(B1);
        Thread.sleep(1100);

        for (int i = 0; i < 1000; i++)
            tracker.register(ByteData.of(i));
        Thread.sleep(1100);

        // This should have an immediate spurious trigger
        this.verifyComplete(f1);
    }

    @Test
    private void testAbsorb() throws Exception {
        final KeyWatchTracker tracker1 = new KeyWatchTracker();
        final ListenableFuture<?> f1 = tracker1.register(B1);

        final KeyWatchTracker tracker2 = new KeyWatchTracker();
        final ListenableFuture<?> f2a = tracker2.register(B1);
        final ListenableFuture<?> f2b = tracker2.register(B2);

        final KeyWatchTracker tracker3 = new KeyWatchTracker();
        final ListenableFuture<?> f3a = tracker3.register(B2);
        final ListenableFuture<?> f3b = tracker3.register(B3);

        tracker1.absorb(tracker2);
        tracker3.absorb(tracker1);

        // Triggering absorbed trackers should have no effect because they're empty
        tracker1.triggerAll();
        tracker2.triggerAll();

        this.verifyNotComplete(f1);
        this.verifyNotComplete(f2a);
        this.verifyNotComplete(f2b);
        this.verifyNotComplete(f3a);
        this.verifyNotComplete(f3b);

        tracker3.trigger(B2);

        this.verifyNotComplete(f1);
        this.verifyNotComplete(f2a);
        this.verifyComplete(f2b);
        this.verifyComplete(f3a);
        this.verifyNotComplete(f3b);

        tracker3.trigger(B1);

        this.verifyComplete(f1);
        this.verifyComplete(f2a);
        this.verifyComplete(f2b);
        this.verifyComplete(f3a);
        this.verifyNotComplete(f3b);

        tracker3.trigger(B3);

        this.verifyComplete(f1);
        this.verifyComplete(f2a);
        this.verifyComplete(f2b);
        this.verifyComplete(f3a);
        this.verifyComplete(f3b);
    }

    void verifyComplete(ListenableFuture<?> future) throws Exception {
        future.get(100, TimeUnit.MILLISECONDS);
    }

    void verifyNotComplete(ListenableFuture<?> future) throws Exception {
        try {
            future.get(25, TimeUnit.MILLISECONDS);
            assert false;
        } catch (TimeoutException e) {
            // expected
        }
    }
}
