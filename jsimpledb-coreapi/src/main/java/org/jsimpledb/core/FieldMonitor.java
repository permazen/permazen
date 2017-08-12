
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.Comparator;

import org.jsimpledb.kv.KeyRanges;

/**
 * A monitor for changes within a {@link Transaction} of the value of a specific field, as seen through a path of references.
 */
class FieldMonitor {

    /**
     * Sorts instances by length of reference path.
     */
    public static final Comparator<FieldMonitor> SORT_BY_PATH_LENGTH = Comparator.comparingInt(m -> m.path.length);

    final int storageId;
    final int[] path;
    final KeyRanges[] filters;
    final Object listener;

    /**
     * Constructor.
     *
     * @param storageId storage ID of the field to monitor
     * @param path path of references to {@code field}; negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener listener to notify
     */
    FieldMonitor(int fieldStorageId, int[] path, KeyRanges[] filters, Object listener) {
        Preconditions.checkArgument(path != null, "null path");
        Preconditions.checkArgument(listener != null, "null listener");
        Preconditions.checkArgument(filters == null || filters.length == path.length + 1, "wrong filters length");
        this.storageId = fieldStorageId;
        this.filters = filters != null ? filters.clone() : null;
        this.path = path.clone();
        this.listener = listener;
    }

    /**
     * @param step step in the path, going backwards
     */
    public int getStorageId(int step) {
        return this.path[this.path.length - 1 - step];
    }

    /**
     * @param step step in the path, going backwards
     */
    public KeyRanges getFilter(int step) {
        return this.filters != null ? this.filters[this.path.length - step] : null;
    }

    /**
     * Get the filter on the target (i.e., changing) object type.
     */
    public KeyRanges getTargetFilter() {
        return this.getFilter(0);
    }

// Object

    @Override
    public int hashCode() {
        return this.storageId
          ^ Arrays.hashCode(this.path)
          ^ Arrays.hashCode(this.filters)
          ^ this.listener.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final FieldMonitor that = (FieldMonitor)obj;
        return this.storageId == that.storageId
          && Arrays.equals(this.path, that.path)
          && Arrays.equals(this.filters, that.filters)
          && this.listener.equals(that.listener);
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

