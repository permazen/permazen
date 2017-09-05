
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import java.util.Arrays;

import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyFilterUtil;
import io.permazen.kv.KeyRanges;
import io.permazen.util.ByteUtil;

/**
 * Common superclass for index view classes. Represents an index with optional filters on each field in the index.
 */
abstract class AbstractIndexView {

    final byte[] prefix;                            // prefix that is always expected and skipped over
    final boolean prefixMode;                       // whether this instance requires prefix mode (i.e., entire key not consumed)
    final FieldType<?>[] fieldTypes;
    final KeyFilter[] filters;

    /**
     * Constructor.
     *
     * @param prefix key prefix
     * @param prefixMode true if prefix mode required because {@code fieldTypes} does not extend through the final value
     * @param fieldTypes field types
     * @throws IllegalArgumentException if {@code prefix} is null or empty
     * @throws IllegalArgumentException if {@code filter} is null or empty
     */
    protected AbstractIndexView(byte[] prefix, boolean prefixMode, FieldType<?>... fieldTypes) {
        Preconditions.checkArgument(prefix != null && prefix.length > 0, "null/empty prefix");
        Preconditions.checkArgument(fieldTypes != null, "null fieldTypes");
        this.prefix = prefix;
        this.prefixMode = prefixMode;
        this.fieldTypes = fieldTypes;
        this.filters = new KeyFilter[this.fieldTypes.length];
    }

    /**
     * Copy-ish constructor for filter().
     */
    protected AbstractIndexView(AbstractIndexView original) {
        this.prefix = original.prefix;
        this.prefixMode = original.prefixMode;
        this.fieldTypes = original.fieldTypes;
        this.filters = original.filters.clone();
    }

    public KeyFilter getFilter(int index) {
        return this.filters[index];
    }

    public boolean hasFilters() {
        for (KeyFilter filter : this.filters) {
            if (filter != null)
                return true;
        }
        return false;
    }

    public AbstractIndexView filter(int index, KeyFilter filter) {
        if (filter == null)
            throw new IndexOutOfBoundsException("null filter");
        if (filter instanceof KeyRanges && ((KeyRanges)filter).isFull())
            return this;
        if (this.filters[index] != null)
            filter = KeyFilterUtil.intersection(filter, this.filters[index]);
        final AbstractIndexView copy = this.copy();
        copy.filters[index] = filter;
        return copy;
    }

    protected abstract AbstractIndexView copy();

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[prefix=" + ByteUtil.toString(this.prefix)
          + ",prefixMode=" + prefixMode
          + ",fieldTypes=" + Arrays.asList(this.fieldTypes)
          + (this.hasFilters() ? ",filters=" + Arrays.asList(this.filters) : "")
          + "]";
    }
}

