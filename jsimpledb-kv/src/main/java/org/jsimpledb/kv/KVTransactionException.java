
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

import com.google.common.base.Throwables;

import java.util.Arrays;

import org.jsimpledb.util.ThrowableUtil;

/**
 * Thrown when an operation on a {@link KVTransaction} fails.
 */
@SuppressWarnings("serial")
public class KVTransactionException extends KVDatabaseException {

    private final KVTransaction kvt;

    public KVTransactionException(KVTransaction kvt) {
        super(kvt.getKVDatabase());
        this.kvt = kvt;
    }

    public KVTransactionException(KVTransaction kvt, Throwable cause) {
        super(kvt.getKVDatabase(), cause);
        this.kvt = kvt;
    }

    public KVTransactionException(KVTransaction kvt, String message) {
        super(kvt.getKVDatabase(), message);
        this.kvt = kvt;
    }

    public KVTransactionException(KVTransaction kvt, String message, Throwable cause) {
        super(kvt.getKVDatabase(), message, cause);
        this.kvt = kvt;
    }

    /**
     * Get the {@link KVTransaction} that generated this exception.
     *
     * @return the associated transaction
     */
    public KVTransaction getTransaction() {
        return this.kvt;
    }

    /**
     * Create a duplicate of this exception, with the current thread's stack frames prepended.
     *
     * <p>
     * This allows the "same" exception to be thrown multiple times from different locations
     * with different outer stack frames.
     *
     * @return duplicate of this exception with the current thread's stack frame context
     * @see org.jsimpledb.util.ThrowableUtil#prependCurrentStackTrace
     */
    public KVTransactionException duplicate() {

        // Get current thread's stack frames
        StackTraceElement[] frames = new Throwable().getStackTrace();

        // Remove frame for KVTransactionException.duplicate()
        if (frames.length > 1)
            frames = Arrays.copyOfRange(frames, 1, frames.length);

        // Add current thread's stack frames to this exception's stack frames
        frames = ThrowableUtil.appendStackFrames(this, frames);

        // Create and configure new instance
        return this.duplicate(frames);
    }

    /**
     * Create a duplicate of this exception, but with its stack frames replaced by the given stack frames.
     *
     * <p>
     * The implementation in {@link KVTransactionException} requires that this instance's class contains
     * a public constructor in the form of {@link #KVTransactionException(KVTransaction, String, Throwable)};
     * subclasses that don't have such a constructor must override this method.
     *
     * @param frames stack frames to use
     * @return duplicate of this exception, but with the given stack frames
     */
    protected KVTransactionException duplicate(StackTraceElement[] frames) {
        final KVTransactionException e;
        try {
            e = this.getClass().getConstructor(KVTransaction.class, String.class, Throwable.class)
              .newInstance(this.kvt, this.getMessage(), this.getCause());
        } catch (Exception e2) {
            throw Throwables.propagate(e2);
        }
        e.setStackTrace(frames);
        return e;
    }
}

