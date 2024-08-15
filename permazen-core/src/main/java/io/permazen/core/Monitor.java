
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KeyRanges;

import java.util.Arrays;

/**
 * Represents a listener that is monitoring for some change in an object found through a path of references.
 *
 * @param <L> the listener type
 */
abstract class Monitor<L> {

    final int[] path;
    final KeyRanges[] filters;
    final L listener;

    /**
     * Constructor.
     *
     * @param path path of references to {@code field}; negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener listener to notify
     */
    Monitor(int[] path, KeyRanges[] filters, L listener) {
        Preconditions.checkArgument(path != null, "null path");
        Preconditions.checkArgument(listener != null, "null listener");
        Preconditions.checkArgument(filters == null || filters.length == path.length + 1, "wrong filters length");
        this.filters = filters != null ? filters.clone() : null;
        this.path = path.clone();
        this.listener = listener;
    }

    /**
     * Get the storage ID for the reference field at the specified step in the path, starting from the end.
     *
     * @param step step in the path, going backwards
     * @return reference field storage ID, or null if none
     */
    public int getStorageId(int step) {
        return this.path[this.path.length - 1 - step];
    }

    /**
     * Get the filter for the reference field at the specified step in the path, starting from the end.
     *
     * @param step step in the path, going backwards
     * @return filter, or null if none
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
        return this.getClass().hashCode()
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
        final Monitor<?> that = (Monitor<?>)obj;
        return Arrays.equals(this.path, that.path)
          && Arrays.equals(this.filters, that.filters)
          && this.listener.equals(that.listener);
    }
}
