
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import io.permazen.kv.KeyRanges;

import java.util.Arrays;

/**
 * Represents a listener that is monitoring for modification of some field in an object found through a path of references.
 */
final class FieldMonitor<L> extends Monitor<L> {

    final int storageId;

    /**
     * Constructor.
     *
     * @param storageId storage ID of the field to monitor
     * @param path path of references to {@code field}; negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener listener to notify
     */
    FieldMonitor(int storageId, int[] path, KeyRanges[] filters, L listener) {
        super(path, filters, listener);
        Preconditions.checkArgument(storageId > 0, "invalid storageId");
        this.storageId = storageId;
    }

// Object

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.storageId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final FieldMonitor<?> that = (FieldMonitor<?>)obj;
        return this.storageId == that.storageId;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[storageId=" + this.storageId
          + ",path=" + Ints.asList(this.path)
          + ",filters=" + Arrays.asList(this.filters)
          + ",listener=" + this.listener
          + "]";
    }
}
