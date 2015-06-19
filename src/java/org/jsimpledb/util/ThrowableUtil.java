
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Preconditions;

/**
 * Utility methods dealing with {@link Throwable}s.
 */
public final class ThrowableUtil {

    private ThrowableUtil() {
    }

    /**
     * Mask a checked exception and throw it as an unchecked exception.
     *
     * @param t any exception
     * @param <T> unchecked exception type
     * @return never
     * @throws T always
     */
    @SuppressWarnings("unchecked")
    public static <T extends Throwable> T maskException(Throwable t) throws T {
        throw (T)t;
    }

    /**
     * Prepend stack frames from the current thread onto the given exception's stack frames.
     *
     * <p>
     * This is used to re-throw an exception created in another thread without losing the stack frame information
     * associated with the current thread.
     *
     * @param t exception from another thread
     * @throws IllegalArgumentException if {@code t} is null
     */
    public static void prependCurrentStackTrace(Throwable t) {
        Preconditions.checkArgument(t != null, "null t");
        final StackTraceElement[] innerFrames = t.getStackTrace();
        final StackTraceElement[] outerFrames = new Throwable().getStackTrace();
        final StackTraceElement[] frames = new StackTraceElement[innerFrames.length + Math.max(outerFrames.length, 0)];
        System.arraycopy(innerFrames, 0, frames, 0, innerFrames.length);
        for (int i = 0; i < outerFrames.length; i++)
            frames[innerFrames.length + i] = outerFrames[i];
        t.setStackTrace(frames);
    }
}

