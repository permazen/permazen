
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import com.google.common.base.Preconditions;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

/**
 * One shot timer that {@linkplain RaftKVDatabase#requestService requests} a {@link Service} on expiration.
 *
 * <p>
 * This implementation avoids any race conditions between scheduling, firing, and cancelling.
 */
class Timer {

    private final RaftKVDatabase raft;
    private final Logger log;
    private final String name;
    private final Service service;

    private ScheduledFuture<?> future;
    private PendingTimeout pendingTimeout;                  // non-null IFF timeout has not been handled yet
    private Timestamp timeoutDeadline;

    Timer(final RaftKVDatabase raft, final String name, final Service service) {
        assert raft != null;
        assert name != null;
        assert service != null;
        this.raft = raft;
        this.log = this.raft.logger;
        this.name = name;
        this.service = service;
    }

    /**
     * Stop timer if running.
     *
     * @throws IllegalStateException if the lock object is not locked
     */
    public void cancel() {

        // Sanity check
        assert Thread.holdsLock(this.raft);

        // Cancel existing timer, if any
        if (this.future != null) {
            this.future.cancel(false);
            this.future = null;
        }

        // Ensure the previously scheduled action does nothing if case we lose the cancel() race condition
        this.pendingTimeout = null;
        this.timeoutDeadline = null;
    }

    /**
     * (Re)schedule this timer. Discards any previously scheduled timeout.
     *
     * @param delay delay before expiration in milliseonds
     * @return true if restarted, false if executor rejected the task
     * @throws IllegalStateException if the lock object is not locked
     */
    public void timeoutAfter(int delay) {

        // Sanity check
        assert Thread.holdsLock(this.raft);
        Preconditions.checkArgument(delay >= 0, "delay < 0");

        // Cancel existing timeout action, if any
        this.cancel();
        assert this.future == null;
        assert this.pendingTimeout == null;
        assert this.timeoutDeadline == null;

        // Reschedule new timeout action
        this.timeoutDeadline = new Timestamp().offset(delay);
        if (this.log.isTraceEnabled()) {
            this.raft.trace("rescheduling " + this.name + " for " + this.timeoutDeadline
              + " (" + delay + "ms from now)");
        }
        Preconditions.checkArgument(!this.timeoutDeadline.isRolloverDanger(), "delay too large");
        this.pendingTimeout = new PendingTimeout();
        try {
            this.future = this.raft.serviceExecutor.schedule(this.pendingTimeout, delay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            if (!this.raft.shuttingDown)
                this.raft.warn("can't restart timer", e);
        }
    }

    /**
     * Force timer to expire immediately.
     */
    public void timeoutNow() {
        this.timeoutAfter(0);
    }

    /**
     * Get the deadline.
     *
     * @return timer deadline, or null if not running
     */
    public Timestamp getDeadline() {
        return this.timeoutDeadline;
    }

    /**
     * Determine if this timer has expired and requires service handling, and reset it if so.
     *
     * <p>
     * If this timer is not running, has not yet expired, or has previously expired and this method was already
     * thereafter invoked, false is returned. Otherwise, true is returned, this timer is {@link #cancel}ed (if necessary),
     * and the caller is expected to handle the implied service need.
     *
     * @return true if timer needs handling, false otherwise
     */
    public boolean pollForTimeout() {

        // Sanity check
        assert Thread.holdsLock(this.raft);

        // Has timer expired?
        if (this.pendingTimeout == null || !this.timeoutDeadline.hasOccurred())
            return false;

        // Yes, timer requires service
        if (Timer.this.log.isTraceEnabled())
            this.raft.trace(Timer.this.name + " expired " + -this.timeoutDeadline.offsetFromNow() + "ms ago");
        this.cancel();
        return true;
    }

    /**
     * Determine if this timer is running, i.e., will expire or has expired but
     * {@link #pollForTimeout} has not been invoked yet.
     */
    public boolean isRunning() {
        return this.pendingTimeout != null;
    }

// PendingTimeout

    private class PendingTimeout implements Runnable {

        @Override
        public void run() {
            synchronized (Timer.this.raft) {

                // Avoid cancel() race condition
                if (Timer.this.pendingTimeout != this)
                    return;

                // Trigger service
                Timer.this.raft.requestService(Timer.this.service);
            }
        }
    }
}
