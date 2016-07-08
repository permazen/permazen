
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.util;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyWatchTrackerTest extends TestSupport {

    private static final byte[] B1 = new byte[] { (byte)12 };
    private static final byte[] B2 = new byte[] { (byte)34 };
    private static final byte[] B3 = new byte[] { (byte)56 };

    @Test
    private void testTrigger1() throws Exception {
        final KeyWatchTracker tracker = new KeyWatchTracker();

        final ListenableFuture<?> f1 = tracker.register(B1);
        final boolean[] flag = new boolean[1];
        f1.addListener(new Runnable() {
            @Override
            public void run() {
                flag[0] = true;
            }
        }, MoreExecutors.directExecutor());

        tracker.trigger(B2);

        // Should still block
        try {
            f1.get(100, TimeUnit.MILLISECONDS);
            assert false;
        } catch (TimeoutException e) {
            // expected
        }

        tracker.trigger(B1);

        // Now should have an immediate trigger
        f1.get(100, TimeUnit.MILLISECONDS);
        Assert.assertTrue(flag[0]);
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

        for (int i = 0; i < futures1.length; i++)
            futures1[i].get(10, TimeUnit.MILLISECONDS);

        for (int i = 0; i < futures2.length; i++) {
            try {
                futures2[i].get(10, TimeUnit.MILLISECONDS);
                assert false;
            } catch (TimeoutException e) {
                // expected
            }
        }
    }

    @Test
    private void testCapacity() throws Exception {
        final KeyWatchTracker tracker = new KeyWatchTracker(2, 99999, false);
        final ListenableFuture<?> f1 = tracker.register(B1);
        final ListenableFuture<?> f2 = tracker.register(B2);
        final ListenableFuture<?> f3 = tracker.register(B3);

        // This should have an immediate spurious trigger
        f1.get(100, TimeUnit.MILLISECONDS);

        // These should still block
        try {
            f2.get(100, TimeUnit.MILLISECONDS);
            assert false;
        } catch (TimeoutException e) {
            // expected
        }
        try {
            f3.get(100, TimeUnit.MILLISECONDS);
            assert false;
        } catch (TimeoutException e) {
            // expected
        }
    }

    @Test
    private void testMaxLifetime() throws Exception {
        final KeyWatchTracker tracker = new KeyWatchTracker(99999, 1, false);
        final ListenableFuture<?> f1 = tracker.register(B1);
        Thread.sleep(1100);

        for (int i = 0; i < 1000; i++)
            tracker.register(new byte[] { (byte)i });
        Thread.sleep(1100);

        // This should have an immediate spurious trigger
        f1.get(250, TimeUnit.MILLISECONDS);
    }
}
