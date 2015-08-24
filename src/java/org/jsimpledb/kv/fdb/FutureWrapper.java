
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.fdb;

import com.foundationdb.async.PartialFuture;
import com.google.common.base.Preconditions;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Wrapper that provdies a view of a {@link PartialFuture} as a {@link Future}.
 */
class FutureWrapper<V> implements Future<V> {

    private static final int WAIT_GRANULARITY_MILLIS = 20;

    private final PartialFuture<V> future;

    private volatile boolean cancelled;

    public FutureWrapper(PartialFuture<V> future) {
        Preconditions.checkArgument(future != null, "null future");
        this.future = future;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (this.future.isDone())
            return false;
        this.cancelled = true;
        this.future.cancel();                           // this is racey but the best we can do
        return true;
    }

    @Override
    public boolean isCancelled() {
        return this.cancelled;
    }

    @Override
    public boolean isDone() {
        return this.future.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        while (true) {
            try {
                return this.get(1, TimeUnit.DAYS);
            } catch (TimeoutException e) {
                continue;
            }
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        final long deadline = System.nanoTime() + unit.toNanos(timeout);
        do {

            // If not done, sleep briefly and try again
            if (!this.future.isDone()) {
                Thread.sleep(WAIT_GRANULARITY_MILLIS);
                continue;
            }

            // Get result, or exception
            try {
                return this.future.get();
            } catch (Throwable t) {
                throw new ExecutionException(t);
            }
        } while (System.nanoTime() - deadline < 0);

        // We timed out
        throw new TimeoutException();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final FutureWrapper<?> that = (FutureWrapper<?>)obj;
        return this.future.equals(that.future);
    }

    @Override
    public int hashCode() {
        return this.future.hashCode();
    }
}

