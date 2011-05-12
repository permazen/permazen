
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.util.HashSet;

/**
 * A place for threads to be parked and unparked.
 */
public class ThreadParkingLot {

    private final HashSet<Thread> parkedThreads = new HashSet<Thread>();

    /**
     * Park the current thread. Execution will halt until {@link #unpark} is invoked
     * with the current thread, the given timeout expires, or the current thread is
     * interrupted.
     *
     * @param timeout maximum time to stay parked, or zero to park indefinitely
     * @return {@code true} if the thread was unparked by another thread, {@code false} if the timeout expired
     * @throws IllegalArgumentException if timeout is negative
     * @throws InterruptedException if the current thread is interrupted
     */
    public synchronized boolean park(long timeout) throws InterruptedException {
        final Thread thread = Thread.currentThread();
        this.parkedThreads.add(thread);
        try {
            return TimedWait.wait(this, timeout, new Predicate() {
                @Override
                public boolean test() {
                    return !ThreadParkingLot.this.parkedThreads.contains(thread);
                }
            });
        } finally {
            this.parkedThreads.remove(thread);
        }
    }

    /**
     * Unpark the specified thread.
     *
     * @param thread the thread to unpark
     * @return {@code true} if {@code thread} was successfully unparked, {@code false} if {@code thread}
     *  is not parked on this instance
     */
    public synchronized boolean unpark(Thread thread) {
        boolean wasParked = this.parkedThreads.remove(thread);
        if (wasParked)
            this.notifyAll();
        return wasParked;
    }

    /**
     * Determine if the given thread is parked on this instance.
     */
    public synchronized boolean isParked(Thread thread) {
        return this.parkedThreads.contains(thread);
    }
}

