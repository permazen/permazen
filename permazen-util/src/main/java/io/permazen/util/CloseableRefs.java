
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds a count of references to a {@link Closeable} resource and only {@link Closeable#close close()}'s
 * it when the reference count reaches zero.
 *
 * <p>
 * Instances are thread safe.
 *
 * @param <C> target object type
 */
@ThreadSafe
public class CloseableRefs<C extends Closeable> {

    protected final C target;

    private final AtomicInteger refs = new AtomicInteger(1);

    /**
     * Constructor.
     *
     * <p>
     * Initially the reference count is set to one.
     *
     * @param target target instance
     */
    public CloseableRefs(C target) {
        Preconditions.checkArgument(target != null, "null target");
        this.target = target;
    }

    /**
     * Get the underlying target instance, which must not yet be closed.
     *
     * <p>
     * The returned value should <i>not</i> be closed; that will happen automatically
     * when the reference count goes to zero.
     *
     * @return the underlying {@link Closeable}
     * @throws IllegalStateException if reference count is already zeroed
     */
    public C getTarget() {
        Preconditions.checkState(this.refs.get() > 0, "no longer referenced");
        return this.target;
    }

    /**
     * Increment reference count.
     *
     * @throws IllegalStateException if reference count is already zeroed
     */
    public void ref() {
        this.refs.updateAndGet(refs -> {
            Preconditions.checkState(refs > 0, "no longer referenced");
            Preconditions.checkState(refs < Integer.MAX_VALUE, "too many references");
            return refs + 1;
        });
    }

    /**
     * Decrement reference count.
     *
     * @throws IllegalStateException if reference count is already zeroed
     */
    public void unref() {
        final int newRefs = this.refs.updateAndGet(refs -> {
            Preconditions.checkState(refs > 0, "no longer referenced");
            return refs - 1;
        });
        if (newRefs == 0) {
            try {
                this.target.close();
            } catch (IOException e) {
                this.handleCloseException(e);
            }
        }
    }

    /**
     * Get current reference count.
     *
     * @return the current number of references to this instance
     */
    public int refs() {
        return this.refs.get();
    }

    /**
     * Handle an exception being thrown by {@link Closeable#close} when closing the target.
     *
     * <p>
     * The implementation in {@link CloseableRefs} just logs the error.
     *
     * @param e exception thrown when closing the target object
     */
    protected void handleCloseException(IOException e) {
        this.getLogger().warn("error closing {}", this.target, e);
    }

    /**
     * Get the {@link Logger} to use for logging errors.
     *
     * <p>
     * The implementation returns the logger associated with {@code this.target.getClass()}.
     */
    protected Logger getLogger() {
        return LoggerFactory.getLogger(this.target.getClass());
    }

    /**
     * Check that the reference count is zero before finalization.
     *
     * <p>
     * If not, that means there was a leak and so the underlying {@link Closeable}
     * is closed and an error is logged.
     */
    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable {
        try {
            final int remaining = this.refs.get();
            if (remaining > 0) {
                this.getLogger().warn(this.target + " leaked with " + remaining + " remaining reference(s)");
                this.refs.set(0);
                this.target.close();
            }
        } finally {
            super.finalize();
        }
    }
}
