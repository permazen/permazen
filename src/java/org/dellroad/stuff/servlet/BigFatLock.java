
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.servlet;

import java.io.IOException;
import java.util.concurrent.Callable;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Class for serializing processing using a big fat lock.
 *
 * <p>
 * The lock can be acquired using {@link #runWithLock}. This class can also be used as a servlet filter.
 *
 * <p>
 * Trace-level logging for displaying lock acquire and release events is included.
 */
public final class BigFatLock extends OncePerRequestFilter {

    private static final Object BIG_FAT_LOCK = new BigFatLock();
    private static final Logger LOG = LoggerFactory.getLogger(BigFatLock.class);

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
      FilterChain filterChain) throws ServletException, IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("thread " + Thread.currentThread() + " waiting for " + BIG_FAT_LOCK);
            synchronized (BIG_FAT_LOCK) {
                LOG.trace("thread " + Thread.currentThread() + " acquired " + BIG_FAT_LOCK);
                try {
                    filterChain.doFilter(request, response);
                } finally {
                    LOG.trace("thread " + Thread.currentThread() + " releasing " + BIG_FAT_LOCK);
                }
            }
        } else {
            synchronized (BIG_FAT_LOCK) {
                filterChain.doFilter(request, response);
            }
        }
    }

    /**
     * Invoke the given action after acquiring the big fat lock.
     */
    public static void runWithLock(Runnable action) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("thread " + Thread.currentThread() + " waiting for " + BIG_FAT_LOCK);
            synchronized (BIG_FAT_LOCK) {
                LOG.trace("thread " + Thread.currentThread() + " acquired " + BIG_FAT_LOCK);
                try {
                    action.run();
                } finally {
                    LOG.trace("thread " + Thread.currentThread() + " releasing " + BIG_FAT_LOCK);
                }
            }
        } else {
            synchronized (BIG_FAT_LOCK) {
                action.run();
            }
        }
    }

    /**
     * Invoke the given action after acquiring the big fat lock.
     */
    public static <V> V runWithLock(Callable<V> action) throws Exception {
        if (LOG.isTraceEnabled()) {
            LOG.trace("thread " + Thread.currentThread() + " waiting for " + BIG_FAT_LOCK);
            synchronized (BIG_FAT_LOCK) {
                LOG.trace("thread " + Thread.currentThread() + " acquired " + BIG_FAT_LOCK);
                try {
                    return action.call();
                } finally {
                    LOG.trace("thread " + Thread.currentThread() + " releasing " + BIG_FAT_LOCK);
                }
            }
        } else {
            synchronized (BIG_FAT_LOCK) {
                return action.call();
            }
        }
    }

    /**
     * Verify that the current thread is holding the big fat lock.
     */
    public static boolean isLockHeld() {
        return Thread.holdsLock(BIG_FAT_LOCK);
    }

    @Override
    public String toString() {
        return "BigFatLock@" + System.identityHashCode(this);
    }
}

