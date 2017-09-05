
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.rocksdb;

import com.google.common.base.Preconditions;

import org.rocksdb.RocksObject;

/**
 * Utility methods for use with RocksDB.
 */
public final class RocksDBUtil {

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
        return obj.isOwningHandle();
    }
}

