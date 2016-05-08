
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.rocksdb;

import com.google.common.base.Preconditions;

import java.lang.reflect.Method;

import org.rocksdb.RocksObject;

/**
 * Utility methods for use with RocksDB.
 */
public final class RocksDBUtil {

    private static Method rocksObjectInitializedMethod;

    private RocksDBUtil() {
    }

    /**
     * Determine whether the given {@link RocksObject} is still valid, i.e., has not been deposed.
     *
     * @param obj object to check
     * @return true if {@code obj} is still valid
     * @throws IllegalArgumentException if {@code obj} is null
     */
    public static boolean isInitialized(RocksObject obj) {
        Preconditions.checkArgument(obj != null, "null obj");
        try {
            if (RocksDBUtil.rocksObjectInitializedMethod == null) {
                RocksDBUtil.rocksObjectInitializedMethod = RocksObject.class.getDeclaredMethod("isInitialized");
                RocksDBUtil.rocksObjectInitializedMethod.setAccessible(true);
            }
            return (Boolean)RocksDBUtil.rocksObjectInitializedMethod.invoke(obj);
        } catch (Exception e) {
            throw new RuntimeException("internal error", e);
        }
    }
}

