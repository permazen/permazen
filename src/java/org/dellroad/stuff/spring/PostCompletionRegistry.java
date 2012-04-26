
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry of post-completion callbacks.
 *
 * <p>
 * Instances of this class are not thread safe.
 *
 * @see PostCompletion
 */
public class PostCompletionRegistry {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final LinkedList<Action> actionList = new LinkedList<Action>();

    private int refs;

    /**
     * Register a callback to be executed upon successful completion only.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><pre>
     *  {@link #add(Runnable, boolean) add}(action, false);
     *  </pre></blockquote>
     *
     * @param action action to invoke
     * @throws IllegalArgumentException if {@code action} is null
     */
    public void add(Runnable action) {
        this.add(action, false);
    }

    /**
     * Register a callback to be executed upon completion.
     *
     * @param action action to invoke
     * @param always true to execute the action even upon unsuccessful completion (i.e., exception thrown)
     * @throws IllegalArgumentException if {@code action} is null
     */
    public void add(Runnable action, boolean always) {
        this.actionList.add(new Action(action, always));
    }

    /**
     * Register a callback to be executed upon successful completion only.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><pre>
     *  {@link #add(Callable, boolean) add}(action, false);
     *  </pre></blockquote>
     *
     * @param action action to invoke
     * @throws IllegalArgumentException if {@code action} is null
     */
    public void add(Callable<?> action) {
        this.add(action, false);
    }

    /**
     * Register a callback to be executed upon completion.
     *
     * @param action action to invoke
     * @param always true to execute the action even upon unsuccessful completion (i.e., exception thrown)
     * @throws IllegalArgumentException if {@code action} is null
     */
    public void add(Callable<?> action, boolean always) {
        this.actionList.add(new Action(action, always));
    }

    /**
     * Invoke all registered actions using the given {@link Executor}.
     * Actions are invoked in the order they were originally added, and as each action is executed
     * it is removed from the head of the list.
     *
     * <p>
     * If {@code successful} is false, then only those actions registered to always run are invoked.
     *
     * <p>
     * Exceptions thrown are logged as errors to the {@link #log}, and then execution proceeds with the following action.
     *
     * @param executor used to execute registered actions
     * @param successful true if completion was successful, false otherwise
     * @throws IllegalArgumentException if {@code executor} is null
     */
    public void execute(Executor executor, boolean successful) {
        if (executor == null)
            throw new IllegalArgumentException("null executor");
        if (this.actionList.isEmpty()) {
            if (this.log.isDebugEnabled()) {
                this.log.debug("no registered post-completion callback(s) to execute after "
                  + (successful ? "a " : "an un") + "successful invocation");
            }
            return;
        }
        if (this.log.isDebugEnabled()) {
            this.log.debug("executing " + this.actionList.size() + " registered post-completion callback(s) after "
              + (successful ? "a " : "an un") + "successful invocation");
        }
        for (Action action; (action = this.actionList.pollFirst()) != null; ) {
            if (!successful && !action.isAlways()) {
                if (this.log.isDebugEnabled())
                    this.log.debug("not executing success-only post-completion callback " + action);
                continue;
            }
            try {
                if (this.log.isDebugEnabled())
                    this.log.debug("executing post-completion callback " + action);
                action.invoke(executor);
                if (this.log.isDebugEnabled())
                    this.log.debug("successfully executed post-completion callback " + action);
            } catch (ThreadDeath t) {
                throw t;
            } catch (Throwable t) {
                if (t instanceof WrapException)
                    t = ((WrapException)t).getException();
                this.log.error("exception thrown by post-completion callback " + action, t);
            }
        }
    }

    /**
     * Add a reference.
     */
    void ref() {
        this.refs++;
    }

    /**
     * Remove a reference.
     *
     * @return true if any references remain, otherwise false
     */
    boolean unref() {
        if (this.refs <= 0)
            throw new IllegalStateException("no more references");
        return --this.refs > 0;
    }

    // Represents a single registered action
    private static class Action {

        private final Runnable action;
        private final Object toStringObject;
        private final boolean always;

        public Action(Runnable action, boolean always) {
            if (action == null)
                throw new IllegalArgumentException("null action");
            this.action = action;
            this.toStringObject = action;
            this.always = always;
        }

        public Action(final Callable<?> action, boolean always) {
            if (action == null)
                throw new IllegalArgumentException("null action");
            this.action = new Runnable() {
                @Override
                public void run() {
                    try {
                        action.call();
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new WrapException(e);
                    }
                }
            };
            this.toStringObject = action;
            this.always = always;
        }

        public boolean isAlways() {
            return this.always;
        }

        public void invoke(Executor executor) throws Exception {
            executor.execute(this.action);
        }

        @Override
        public String toString() {
            return this.toStringObject.toString();
        }
    }

    @SuppressWarnings("serial")
    private static class WrapException extends RuntimeException {

        public WrapException(Exception e) {
            super(e);
        }

        public Exception getException() {
            return (Exception)this.getCause();
        }
    }
}

