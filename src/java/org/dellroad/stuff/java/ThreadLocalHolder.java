
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.util.concurrent.Callable;

/**
 * Manages a thread local whose lifetime matches the duration of some method call.
 *
 * <p>
 * This class is useful for this common pattern:
 * <ul>
 *  <li>A thread local variable is instantiated by some initial method call and has an intended
 *      lifetime matching the duration of that method call;</li>
 *  <li>The thread local variable is accessible from some other nested method calls in the same thread,
 *      as long as the initial method call is still executing;</li>
 *  <li>The thread local variable is removed (and optionally cleaned up) when the initial method call exits,
 *      whether successfully or not.</li>
 * </ul>
 * </p>
 *
 * <p>
 * Example:
 * <blockquote><pre>
 * public class Activity {
 *
 *     private static final ThreadLocalHolder&lt;Activity&gt; CURRENT_ACTIVITY = new ThreadLocalHolder&lt;Activity&gt;();
 *
 *     public void perform(final Object parameter) {
 *         CURRENT_ACTIVITY.invoke(this, new Runnable() {
 *             &#64;Override
 *             public void run() {
 *                 // do whatever with parameter
 *             }
 *         });
 *     }
 *
 *     /**
 *      * Get the current activity being performed.
 *      *
 *      * @throws IllegalStateException if there is no current activity
 *      *&#47;
 *     public static Activity currentActivity() {
 *         return CURRENT_ACTIVITY.require();
 *     }
 * }
 * </pre></blockquote>
 * </p>
 *
 * @param <T> the type of the thread local variable
 */
public class ThreadLocalHolder<T> {

    private final ThreadLocal<T> threadLocal;

    /**
     * Conveninece constructor.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><code>new ThreadLocalHolder&lt;T&gt;(new ThreadLocal&lt;T&gt;())</code></blockquote>
     */
    public ThreadLocalHolder() {
        this(new ThreadLocal<T>());
    }

    /**
     * Primary constructor.
     *
     * @param threadLocal the thread local to use
     * @throws IllegalArgumentException if {@code threadLocal} is null
     */
    public ThreadLocalHolder(ThreadLocal<T> threadLocal) {
        if (threadLocal == null)
            throw new IllegalArgumentException("null threadLocal");
        this.threadLocal = threadLocal;
    }

    /**
     * Invoke the given action while making the given thread local variable available via {@link #get} and {@link #require}.
     *
     * <p>
     * If there is already a thread local variable set for the current thread (i.e., we are already executing within
     * an invocation of <code>ThreadLocalHolder.invoke()</code>, then if {@code value} is the exact same Java object
     * (using object equality, not <code>equals()</code>), execution proceeds normally, otherwise an exception is thrown.
     *
     * @param value value for the thread local variable
     * @param action action to invoke
     * @throws IllegalArgumentException if either {@code action} or {@code value} is null
     * @throws IllegalStateException if there is already a thread local variable <code>previous</code>
     *  associated with the current thread and <code>value != previous</code>
     */
    public void invoke(final T value, Runnable action) {
        if (action == null)
            throw new IllegalArgumentException("null action");
        if (value == null)
            throw new IllegalArgumentException("null value");
        final T previousValue = this.threadLocal.get();
        final boolean topLevel = previousValue == null;
        if (!topLevel) {
            if (value != previousValue) {
                throw new IllegalStateException("already executing within an invocation of ThreadLocalHolder.invoke()"
                  + " but with a different value");
            }
        } else
            this.threadLocal.set(value);
        try {
            action.run();
        } finally {
            if (topLevel) {
                this.threadLocal.remove();
                this.destroy(value);
            }
        }
    }

    /**
     * Invoke the given action while making the given thread local variable available via {@link #get} and {@link #require}.
     *
     * <p>
     * If there is already a thread local variable set for the current thread (i.e., we are already executing within
     * an invocation of <code>ThreadLocalHolder.invoke()</code>, then if {@code value} is the exact same Java object
     * (using object equality, not <code>equals()</code>), execution proceeds normally, otherwise an exception is thrown.
     *
     * @param value value for the thread local variable
     * @param action action to invoke
     * @throws IllegalArgumentException if either {@code action} or {@code value} is null
     * @throws IllegalStateException if there is already a thread local variable <code>previous</code>
     *  associated with the current thread and <code>value != previous</code>
     * @throws Exception if {@code action} throws an {@link Exception}
     */
    public <R> R invoke(final T value, Callable<R> action) throws Exception {
        if (action == null)
            throw new IllegalArgumentException("null action");
        if (value == null)
            throw new IllegalArgumentException("null value");
        final T previousValue = this.threadLocal.get();
        final boolean topLevel = previousValue == null;
        if (!topLevel) {
            if (value != previousValue) {
                throw new IllegalStateException("already executing within an invocation of ThreadLocalHolder.invoke()"
                  + " but with a different value");
            }
        } else
            this.threadLocal.set(value);
        try {
            return action.call();
        } finally {
            if (topLevel) {
                this.threadLocal.remove();
                this.destroy(value);
            }
        }
    }

    /**
     * Get the thread local value associated with the current thread, if any.
     *
     * @return the current thread local variable value, or null if not executing
     *  within an invocation of <code>ThreadLocalHolder.invoke()</code>
     */
    public T get() {
        return this.threadLocal.get();
    }

    /**
     * Get the thread local value associated with the current thread; there must be one.
     *
     * @return the current thread local variable value, never null
     * @throws IllegalStateException if the current thread is not running
     *  within an invocation of <code>ThreadLocalHolder.invoke()</code>
     */
    public T require() {
        T value = this.threadLocal.get();
        if (value == null) {
            throw new IllegalStateException("no value associated with the current thread;"
              + " are we executing within an invocation of ThreadLocalHolder.invoke()?");
        }
        return value;
    }

    /**
     * Clean up the thread local value when no longer needed.
     *
     * <p>
     * The implementation in {@link ThreadLocalHolder} does nothing. Subclasses may override if necessary.
     */
    protected void destroy(T value) {
    }
}

