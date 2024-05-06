
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.jshell;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.tuple.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

import jdk.jshell.execution.LoaderDelegate;
import jdk.jshell.execution.LocalExecutionControl;

public class PermazenExecutionControl extends LocalExecutionControl {

    private static final HashMap<ThreadGroup, Tuple2<Session, Method>> INVOCATION_MAP = new HashMap<>();
    private static final Method INVOKE_WRAPPER;
    static {
        try {
            INVOKE_WRAPPER = PermazenExecutionControl.class.getDeclaredMethod("invokeWrapper");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("internal error");
        }
    }

    private final Session session;

// Constructor

    /**
     * Constructor.
     *
     * @param delegate loader delegate
     */
    public PermazenExecutionControl(LoaderDelegate delegate) {
        super(delegate);
        this.session = PermazenJShellShellSession.getCurrent().getPermazenSession();
        Preconditions.checkState(this.session != null, "no session");
    }

// LocalExecutionControl

    // This is a total hack. Our goal is to access the Permazen session from within the thread
    // that is actually invoking the target method (which is different from the current thread here)
    // so that we can open a transaction around the invocation of "method". To accomplish that, we
    // invoke invokeWrapper() instead of "method". Our invokeWrapper() will be invoked in some random
    // thread whose ThreadGroup's parent ThreadGroup is this current thread's ThreadGroup. We use that
    // tenuous connection to retrieve the original method given here and also our Permazen session.
    @Override
    protected String invoke(Method method) throws Exception {
        final Tuple2<Session, Method> info = new Tuple2<>(this.session, method);
        final ThreadGroup lookupKey = Thread.currentThread().getThreadGroup();
        synchronized (INVOCATION_MAP) {
            while (INVOCATION_MAP.containsKey(lookupKey))
                INVOCATION_MAP.wait();
            INVOCATION_MAP.put(lookupKey, info);
        }
        try {
            return super.invoke(INVOKE_WRAPPER);
        } finally {

            // Clean up in case invokeWrapper() is never actually invoked
            final Tuple2<Session, Method> info2;
            synchronized (INVOCATION_MAP) {
                info2 = INVOCATION_MAP.get(lookupKey);
                if (info2 == info)                          // use object equality to ensure it's ours
                    INVOCATION_MAP.remove(lookupKey);
            }
        }
    }

// Internal Methods

    /**
     * Invocation wrapper method.
     *
     * <p>
     * This method is only used internally but is required to be public due to Java access controls.
     */
    public static Object invokeWrapper() throws Throwable {

        // Get the target method info stashed by invoke()
        final ThreadGroup lookupKey = Thread.currentThread().getThreadGroup().getParent();
        final Tuple2<Session, Method> info;
        synchronized (INVOCATION_MAP) {
            info = INVOCATION_MAP.remove(lookupKey);
        }
        if (info == null)
            throw new RuntimeException("internal error: target method info not found");

        // Invoke it within a transaction
        final Session session = info.getValue1();
        final Method method = info.getValue2();
        session.openTransaction(null);
        boolean success = false;
        try {
            Object result = method.invoke(null);
            if (result != null)
                result = new HiddenString(valueString(result));
            success = true;
            return result;
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } finally {
            session.closeTransaction(success);
        }
    }

// HiddenString

    // The reason we need this class is because we want to execute DirectExecutionControl.valueString() inside the transaction.
    // In order to neutralize its redundant execution in DirectExecutionControl.invoke(), we "hide" the already-decoded string.
    private static final class HiddenString {

        private final String string;

        HiddenString(String string) {
            Preconditions.checkState(string != null, "null string");
            this.string = string;
        }

        @Override
        public String toString() {
            return this.string;
        }
    }
}
