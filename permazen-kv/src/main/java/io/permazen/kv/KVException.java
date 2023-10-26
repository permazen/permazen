
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import java.util.Arrays;

import org.dellroad.stuff.java.ThrowableUtil;

/**
 * Superclass of unchecked exceptions relating to {@link KVStore}s, etc.
 */
@SuppressWarnings("serial")
public class KVException extends RuntimeException implements Cloneable {

    public KVException() {
    }

    public KVException(String message) {
        super(message);
    }

    public KVException(Throwable cause) {
        super(cause);
    }

    public KVException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Rethrow this exception, which may have been created in a different thread, in the context of the current thread.
     *
     * <p>
     * This method ensures the current thread's stack frames are included in the thrown exception.
     *
     * <p>
     * This method never returns, but has a return type that can be "thrown" to facilitate source code control flow.
     *
     * @return never
     * @throws KVException always, as a duplicate of this exception with the current thread's stack frame context
     */
    public KVException rethrow() {
        throw this.duplicate();
    }

    /**
     * Create a duplicate of this exception, with the current thread's stack frames prepended.
     *
     * <p>
     * This allows the "same" exception to be thrown multiple times from different locations
     * with different outer stack frames. The {@link #clone} method is used to copy this instance.
     *
     * @return duplicate of this exception with the current thread's stack frame context
     * @see ThrowableUtil#appendStackFrames
     */
    public KVException duplicate() {

        // Get current thread's stack frames
        StackTraceElement[] frames = new Throwable().getStackTrace();

        // Remove frame for KVException.duplicate()
        if (frames.length > 1)
            frames = Arrays.copyOfRange(frames, 1, frames.length);

        // Add current thread's stack frames to this exception's stack frames
        frames = ThrowableUtil.appendStackFrames(this, frames);

        // Create and configure new instance
        final KVException e = this.clone();
        e.setStackTrace(frames);
        return e;
    }

    /**
     * Create a clone of this instance.
     */
    @Override
    public KVException clone() {
        final KVException clone;
        try {
            clone = (KVException)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        clone.setStackTrace(clone.getStackTrace().clone());
        return clone;
    }
}
