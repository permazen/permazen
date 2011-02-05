
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
     * @param obj       object to sleep on; must already be locked
     * @param timeout   wait timeout in milliseconds, or zero for an infinite wait
     * @param predicate predicate to test
     * @return true if the predicate became true before the timeout, otherwise false
     * @throws IllegalArgumentException     if {@code timeout} is negative
     * @throws IllegalMonitorStateException if {@code obj} is not already locked
     * @throws InterruptedException         if the current thread is interrupted
     */
    public static boolean wait(Object obj, long timeout, final Predicate predicate) throws InterruptedException {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout = " + timeout);
        long startTime = timeout > 0 ? System.currentTimeMillis() : 0;
        while (!predicate.test()) {
            if (timeout < 0)
                return false;
            obj.wait(timeout);
            if (timeout > 0) {
                long stopTime = System.currentTimeMillis();
                timeout -= stopTime - startTime;
                if (timeout == 0)               // don't convert the last millisecond into an infinite wait
                    timeout = -1;
                startTime = stopTime;
            }
        }
        return true;
    }
}

