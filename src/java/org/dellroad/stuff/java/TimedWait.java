
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;


/**
 * Utility class for performing timed waits on objects.
 *
 * @see Object#wait
 */
public final class TimedWait {

    private TimedWait() {
    }

    /**
     * Wait (using {@link Object#wait(long) Object.wait()}) up to a given time limit for some predicate to become true.
     * This method correctly handles {@link Object#wait spurious wakeups}, restarting the wait loop as necessary.
     *
     * <p>
     * This method assumes that {@code obj} will be notified whent the predicate becomes true and that the current thread
     * is already synchronized on {@code obj}.
     *
     * <p>
     * This method uses {@link System#nanoTime} instead of {@link System#currentTimeMillis} and so is immune to
     * adjustments in clock time.
     *
     * @param obj       object to sleep on; must already be locked
     * @param timeout   wait timeout in milliseconds, or zero for an infinite wait
     * @param predicate predicate to test
     * @return true if the predicate became true before the timeout, false if the timeout expired
     * @throws IllegalArgumentException     if {@code timeout} is negative
     * @throws IllegalMonitorStateException if {@code obj} is not already locked
     * @throws InterruptedException         if the current thread is interrupted
     */
    public static boolean wait(Object obj, long timeout, final Predicate predicate) throws InterruptedException {

        // Sanity check timeout value
        if (timeout < 0)
            throw new IllegalArgumentException("timeout = " + timeout);

        // Record start time (only if there's a timeout)
        long startTime = timeout > 0 ? System.nanoTime() : 0;

        // Loop waiting for predicate
        while (!predicate.test()) {

            // Did the timeout expire?
            if (timeout < 0)
                return false;

            // Wait for remaining timeout to be woken up
            obj.wait(timeout);

            // If there's a timeout, subtract the time we just waited (rounding to the nearest millisecond)
            if (timeout > 0) {
                long stopTime = System.nanoTime();
                timeout -= (stopTime - startTime + 500000L) / 1000000L;
                if (timeout == 0)               // don't convert the last millisecond into an infinite wait
                    timeout = -1;
                startTime = stopTime;
            }
        }

        // Predicate was true
        return true;
    }
}

