
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.springframework.scheduling.TaskScheduler;

/**
 * Manages a delayed action without race conditions.
 *
 * <p>
 * A "delayed action" is a some action that needs to get done by some time in the future.
 * </p>
 *
 * <p>
 * This class does two things:
 * <ul>
 *  <li>It collapses multiple attempts to schedule the action into a single scheduled action,
 *  i.e., at most one outstanding scheduled action can exist at a time.</li>
 *  <li>It provides a race-free and 100% reliable way to {@link #cancel} a future scheduled action, if any.</li>
 * </ul>
 * </p>
 *
 * <p>
 * The action itself is defined by the subclass' implementation of {@link #run run()}.
 * </p>
 *
 * <p>
 * To avoid races, this class requires the user to supply a <i>locking object</i>. This may either be a normal
 * Java object, in which case normal Java synchronization is used, or a {@link Lock} object. The locking object is
 * used to serialize scheduling activity and action invocation. In other words, the locking object is locked during
 * the execution of {@link #schedule schedule()}, {@link #cancel cancel()}, and {@link #run run()}.
 * </p>
 *
 * <p>
 * Therefore, any time the locking object is locked, the state of this {@link DelayedAction} instance is "frozen"
 * in one of three states: not scheduled, scheduled, or executing (in the latter case, of course the thread doing the
 * executing is the one holding the lock). Therefore, to completely avoid race conditions, user code must <i>itself</i>
 * lock the locking object itself prior to invoking any methods in this class.
 * </p>
 *
 * <p>
 * Typically the most convenient locking object to use is the user's own {@code this} object, which can be locked using a
 * {@code synchronized} method or block.
 * </p>
 *
 * <p>
 * Note: in the case that {@link #run run()} invokes {@link Object#wait Object.wait()} on the locking object, thereby
 * temporarily releasing the lock, to any other methods in this class it will appear as if that execution has already
 * completed.
 */
public abstract class DelayedAction implements Runnable {

    private final Lock lock;
    private final Object objLock;
    private final TaskScheduler taskScheduler;
    private final ScheduledExecutorService executorService;

    private ScheduledFuture future;
    private Date futureDate;

    /**
     * Constructor utitilizing a {@link TaskScheduler} and normal Java object locking.
     *
     * @param lock locking object used to serialize activity, or null for {@code this}
     * @param taskScheduler scheduler object
     * @throws IllegalArgumentException if {@code taskScheduler} is null
     */
    protected DelayedAction(Object lock, TaskScheduler taskScheduler) {
        this(null, lock, taskScheduler, null);
        if (taskScheduler == null)
            throw new IllegalArgumentException("null taskScheduler");
    }

    /**
     * Constructor utitilizing a {@link ScheduledExecutorService} and normal Java object locking.
     *
     * @param lock locking object used to serialize activity, or null for {@code this}
     * @param executorService scheduler object
     * @throws IllegalArgumentException if {@code executorService} is null
     */
    protected DelayedAction(Object lock, ScheduledExecutorService executorService) {
        this(null, lock, null, executorService);
        if (executorService == null)
            throw new IllegalArgumentException("null executorService");
    }

    /**
     * Constructor utitilizing a {@link TaskScheduler} and a {@link Lock} for locking.
     *
     * @param lock locking object used to serialize activity
     * @param taskScheduler scheduler object
     * @throws IllegalArgumentException if {@code lock} is null
     * @throws IllegalArgumentException if {@code taskScheduler} is null
     */
    protected DelayedAction(Lock lock, TaskScheduler taskScheduler) {
        this(lock, null, taskScheduler, null);
        if (lock == null)
            throw new IllegalArgumentException("null lock");
        if (taskScheduler == null)
            throw new IllegalArgumentException("null taskScheduler");
    }

    /**
     * Constructor utitilizing a {@link ScheduledExecutorService} and a {@link Lock} for locking.
     *
     * @param lock locking object used to serialize activity
     * @param executorService scheduler object
     * @throws IllegalArgumentException if {@code lock} is null
     * @throws IllegalArgumentException if {@code executorService} is null
     */
    protected DelayedAction(Lock lock, ScheduledExecutorService executorService) {
        this(lock, null, null, executorService);
        if (lock == null)
            throw new IllegalArgumentException("null lock");
        if (executorService == null)
            throw new IllegalArgumentException("null executorService");
    }

    private DelayedAction(Lock lock, Object objLock, TaskScheduler taskScheduler, ScheduledExecutorService executorService) {
        this.lock = lock;
        this.objLock = lock != null ? null : objLock != null ? objLock : this;
        this.taskScheduler = taskScheduler;
        this.executorService = executorService;
    }

    /**
     * Schedule the delayed action for the given time.
     *
     * <p>
     * More precisely:
     * <ul>
     *  <li>If an action currently executing, before doing anything else this method blocks until it completes;
     *  if this behavior is undesirable, the caller can avoid this behavior by synchronizing on the locking object
     *  prior to invoking this method.</li>
     *  <li>If no action is scheduled, one is scheduled for the given time.</li>
     *  <li>If an action is already scheduled, and the given time is on or after the scheduled time, nothing changes.</li>
     *  <li>If an action is already scheduled, and the given time is prior to the scheduled time,
     *  the action is rescheduled for the earlier time.</li>
     * </ul>
     * </p>
     *
     * <p>
     * The net result is that, for any invocation, this method guarantees exactly one execution of the action will
     * occur approximately on or before the given date; however, multiple invocations of this method prior to action
     * execution can only ever result in a single "shared" action.
     * </p>
     *
     * @param date scheduled execution time (at the latest)
     * @throws IllegalArgumentException if {@code date} is null
     * @throws org.springframework.core.task.TaskRejectedException
     *  if the given task was not accepted for internal reasons (e.g. a pool overload handling policy
     *  or a pool shutdown in progress)
     */
    public void schedule(final Date date) {
        this.runLocked(new Runnable() {
            @Override
            public void run() {
                DelayedAction.this.scheduleWhileLocked(date);
            }
        });
    }
    private void scheduleWhileLocked(final Date date) {

        // Sanity check
        if (date == null)
            throw new IllegalArgumentException("null date");

        // Already scheduled?
        if (this.future != null) {

            // Requested time is after scheduled time? Note: must be ">=", not ">" to ensure monotonically increasing Dates
            if (date.compareTo(this.futureDate) >= 0)
                return;

            // Cancel it
            this.cancel();
        }

        // Schedule it
        this.future = this.schedule(new Runnable() {
            @Override
            public void run() {
                DelayedAction.this.futureInvoked(date);
            }
        }, date);
        this.futureDate = date;
    }

    /**
     * Cancel the future scheduled action, if any.
     *
     * <p>
     * More precisely:
     * <ul>
     *  <li>If an action currently executing, before doing anything else this method blocks until it completes;
     *  if this behavior is undesirable, the caller can avoid this behavior by synchronizing on the locking object
     *  prior to invoking this method.</li>
     *  <li>If an action is scheduled but has not started yet, it is guaranteed not to run.</li>
     *  <li>If no action is scheduled or executing, nothing changes.</li>
     * </ul>
     * </p>
     */
    public void cancel() {
        this.runLocked(new Runnable() {
            @Override
            public void run() {
                DelayedAction.this.cancelWhileLocked();
            }
        });
    }
    private void cancelWhileLocked() {

        // Anything to do?
        if (this.future == null)
            return;

        // Cancel future
        this.future.cancel(false);
        this.future = null;
        this.futureDate = null;
    }

    /**
     * Determine whether there is currently an outstanding scheduled action.
     */
    public boolean isScheduled() {
        final boolean[] result = new boolean[1];
        this.runLocked(new Runnable() {
            @Override
            public void run() {
                result[0] = DelayedAction.this.future != null;
            }
        });
        return result[0];
    }

    /**
     * Get the scheduled time for the outstanding scheduled action, if any.
     *
     * @return oustanding action scheduled time, or null if there is no scheduled action
     */
    public Date getScheduledTime() {
        final Date[] result = new Date[1];
        this.runLocked(new Runnable() {
            @Override
            public void run() {
                result[0] = DelayedAction.this.futureDate;
            }
        });
        return result[0];
    }

    /**
     * Schedule the given action using the task scheduler passed to the constructor.
     * Use of this method does not change the state of this instance.
     *
     * @param action action to perform
     * @param date when to perform it
     * @throws IllegalArgumentException if either parameter is null
     * @throws java.util.concurrent.RejectedExecutionException
     *   if the given task was not accepted for internal reasons (e.g. a pool overload handling
     *  policy or a pool shutdown in progress)
     */
    protected ScheduledFuture<?> schedule(Runnable action, Date date) {
        if (action == null)
            throw new IllegalArgumentException("null action");
        if (date == null)
            throw new IllegalArgumentException("null date");
        if (this.taskScheduler != null)
            return this.taskScheduler.schedule(action, date);
        long now = System.currentTimeMillis();
        long when = date.getTime();
        if (when < now)
            when = now;
        return this.executorService.schedule(action, when - now, TimeUnit.MILLISECONDS);
    }

    // Do the action
    private void futureInvoked(final Date date) {
        this.runLocked(new Runnable() {
            @Override
            public void run() {
                DelayedAction.this.futureInvokedWhileLocked(date);
            }
        });
    }
    private void futureInvokedWhileLocked(Date date) {

        // Handle race condition where future.cancel() fails
        if (this.futureDate != date)
            return;

        // Reset state
        this.future = null;
        this.futureDate = null;

        // Perform action
        this.run();
    }

    // Invoke action while locked
    private void runLocked(Runnable action) {
        if (this.objLock != null) {
            synchronized (this.objLock) {
                action.run();
            }
        } else {
            this.lock.lock();
            try {
                action.run();
            } finally {
                this.lock.unlock();
            }
        }
    }
}

