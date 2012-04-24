
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import java.util.concurrent.Callable;

/**
 * Supports requesting the execution of post-completion callbacks when running within
 * {@link PostCompletionSupport @PostCompletionSupport}-annotated methods.
 *
 * <p>
 * See {@link PostCompletionSupport @PostCompletionSupport} for an example of how this class is normally used.
 *
 * @see PostCompletionSupport
 */
public final class PostCompletion {

    private static final ThreadLocal<PostCompletionRegistry> CURRENT = new ThreadLocal<PostCompletionRegistry>();

    private PostCompletion() {
    }

    /**
     * Register a callback to be executed upon successful completion only.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><pre>
     *  {@link #execute(Runnable, boolean) execute}(action, false);
     *  </pre></blockquote>
     *
     * @param action action to invoke
     * @throws IllegalArgumentException if {@code action} is null
     */
    public static void execute(Runnable action) {
        PostCompletion.execute(action, false);
    }

    /**
     * Register a post-completion callback.
     *
     * @param action action to invoke
     * @param always true to execute the action even upon unsuccessful completion (i.e., exception thrown)
     * @throws IllegalArgumentException if {@code action} is null
     * @throws IllegalStateException if not running within a
     *  {@link PostCompletionSupport @PostCompletionSupport}-annotated method.
     */
    public static void execute(Runnable action, boolean always) {
        PostCompletion.get().add(action, always);
    }

    /**
     * Register a callback to be executed upon successful completion only.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><pre>
     *  {@link #execute(Callable, boolean) execute}(action, false);
     *  </pre></blockquote>
     *
     * @param action action to invoke
     * @throws IllegalArgumentException if {@code action} is null
     */
    public static void execute(Callable action) {
        PostCompletion.execute(action, false);
    }

    /**
     * Register a post-completion callback.
     *
     * @param action action to invoke
     * @param always true to execute the action even upon unsuccessful completion (i.e., exception thrown)
     * @throws IllegalArgumentException if {@code action} is null
     * @throws IllegalStateException if not running within a
     *  {@link PostCompletionSupport @PostCompletionSupport}-annotated method.
     */
    public static void execute(Callable action, boolean always) {
        PostCompletion.get().add(action, always);
    }

    static PostCompletionRegistry get() {
        PostCompletionRegistry current = CURRENT.get();
        if (current == null) {
             throw new IllegalStateException("no PostCompletionRegistry instance is associated with the current thread;"
               + " are we executing within a @PostCompletionSupport-annotated method?");
        }
        return current;
    }

    static void push() {
        PostCompletionRegistry current = CURRENT.get();
        if (current == null) {
            current = new PostCompletionRegistry();
            CURRENT.set(current);
        }
        current.ref();
    }

    static boolean pop() {
        PostCompletionRegistry current = PostCompletion.get();
        if (!current.unref()) {
            CURRENT.remove();
            return false;
        }
        return true;
    }
}

