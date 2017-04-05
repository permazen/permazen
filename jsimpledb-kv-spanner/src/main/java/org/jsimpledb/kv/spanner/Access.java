
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.spanner;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This is here due to <a href="https://github.com/GoogleCloudPlatform/google-cloud-java/issues/1627">Issue #1627</a>
 */
final class Access {

    public static final Method TRANSACTION_CONTEXT_COMMIT_METHOD;
    public static final Method TRANSACTION_CONTEXT_ROLLBACK_METHOD;
    public static final Method TRANSACTION_CONTEXT_ENSURE_TXN_METHOD;
    public static final Method TRANSACTION_CONTEXT_COMMIT_TIMESTAMP_METHOD;

    public static final Field POOLED_SESSION_1_RUNNER_FIELD;
    public static final Field TRANSACTION_RUNNER_TXN_FIELD;

    static {
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {

            final Class<?> tc = Class.forName("com.google.cloud.spanner.SpannerImpl$TransactionContextImpl", false, loader);
            TRANSACTION_CONTEXT_COMMIT_METHOD = tc.getDeclaredMethod("commit");
            TRANSACTION_CONTEXT_COMMIT_METHOD.setAccessible(true);
            TRANSACTION_CONTEXT_ROLLBACK_METHOD = tc.getDeclaredMethod("rollback");
            TRANSACTION_CONTEXT_ROLLBACK_METHOD.setAccessible(true);
            TRANSACTION_CONTEXT_ENSURE_TXN_METHOD = tc.getDeclaredMethod("ensureTxn");
            TRANSACTION_CONTEXT_ENSURE_TXN_METHOD.setAccessible(true);
            TRANSACTION_CONTEXT_COMMIT_TIMESTAMP_METHOD = tc.getDeclaredMethod("commitTimestamp");
            TRANSACTION_CONTEXT_COMMIT_TIMESTAMP_METHOD.setAccessible(true);

            final Class<?> tr = Class.forName("com.google.cloud.spanner.SpannerImpl$TransactionRunnerImpl", false, loader);
            TRANSACTION_RUNNER_TXN_FIELD = tr.getDeclaredField("txn");
            TRANSACTION_RUNNER_TXN_FIELD.setAccessible(true);

            final Class<?> ps1 = Class.forName("com.google.cloud.spanner.SessionPool$PooledSession$1", false, loader);
            POOLED_SESSION_1_RUNNER_FIELD = ps1.getDeclaredField("val$runner");
            POOLED_SESSION_1_RUNNER_FIELD.setAccessible(true);

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Access() {
    }

    static Object invoke(Method method, Object instance, Object... params) {
        try {
            return method.invoke(instance, params);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e.getCause());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static Object read(Field field, Object instance) {
        try {
            return field.get(instance);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
