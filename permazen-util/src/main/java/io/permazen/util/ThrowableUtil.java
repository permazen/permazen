
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

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
        t.setStackTrace(ThrowableUtil.appendStackFrames(t, new Throwable().getStackTrace()));
    }

    /**
     * Prepend stack frames from the current thread onto the given exception's stack frames and return the result.
     *
     * @param t original exception
     * @param outerFrames stack frames that should wrap {@code t}'s stack frames
     * @return an array containing {@code t}'s stack frames, followed by {@code outerFrames}
     * @throws IllegalArgumentException if {@code t} or {@code outerFrames} is null
     */
    public static StackTraceElement[] appendStackFrames(Throwable t, StackTraceElement[] outerFrames) {
        Preconditions.checkArgument(t != null, "null t");
        Preconditions.checkArgument(outerFrames != null, "null outerFrames");
        final StackTraceElement[] innerFrames = t.getStackTrace();
        final StackTraceElement[] frames = new StackTraceElement[innerFrames.length + outerFrames.length];
        System.arraycopy(innerFrames, 0, frames, 0, innerFrames.length);
        for (int i = 0; i < outerFrames.length; i++)
            frames[innerFrames.length + i] = outerFrames[i];
        return frames;
    }
}

