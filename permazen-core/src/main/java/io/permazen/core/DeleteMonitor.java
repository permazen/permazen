
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.primitives.Ints;

import io.permazen.kv.KeyRanges;

import java.util.Arrays;

/**
 * Represents a listener that is monitoring for deletion of an object found through a path of references.
 */
final class DeleteMonitor extends Monitor<DeleteListener> {

    /**
     * Constructor.
     *
     * @param storageId storage ID of the field to monitor
     * @param path path of references to {@code field}; negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener listener to notify
     */
    DeleteMonitor(int[] path, KeyRanges[] filters, DeleteListener listener) {
        super(path, filters, listener);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[path=" + Ints.asList(this.path)
          + ",filters=" + Arrays.asList(this.filters)
          + ",listener=" + this.listener
          + "]";
    }
}
