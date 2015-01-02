
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Arrays;

import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;

/**
 * A {@link KeyFilter} based on an {@link AbstractIndexView}. A prefix of the fields is used to filter the key directly,
 * and any remaining fields are used to further filter the key based on the existence of any combined keys (prefix + suffix)
 * in the underlying {@link Transaction}.
 */
class IndexKeyFilter implements KeyFilter {

    private final Transaction tx;
    private final byte[] prefix;                                // prefix to always skip over
    private final FieldType<?>[] fieldTypes;                    // fields to decode after prefix
    private final KeyFilter[] filters;                          // filters to apply to fields
    private final int prefixLen;                                // how many fields are mandatory

    private final FieldTypesFilter prefixFilter;

    /**
     * Constructor from an index view.
     *
     * @param tx transaction
     * @param indexView associated index view
     * @param prefixLen the number of fields in {@code indexView} that are considered mandatory
     * @throws IllegalArgumentException if {@code tx} or {@code or indexView} is null
     * @throws IllegalArgumentException if {@code prefixLen} is zero or out of range
     */
    public IndexKeyFilter(Transaction tx, AbstractIndexView indexView, int prefixLen) {
        this(tx, indexView.prefix, indexView.fieldTypes, indexView.filters, prefixLen);
    }

    /**
     * Primary constructor.
     *
     * @param tx transaction
     * @param prefix prefix before first field type
     * @param fieldTypes one or more index field types
     * @param filters filters corresponding to {@code fieldTypes}
     * @param prefixLen the number of fields in {@code fieldTypes} that are considered mandatory
     * @throws IllegalArgumentException if {@code tx} is null
     * @throws IllegalArgumentException if {@code fieldTypes} is null or empty
     * @throws IllegalArgumentException if {@code prefix} is null or empty
     * @throws IllegalArgumentException if {@code prefixLen} is zero or out of range
     * @throws IllegalArgumentException if {@code filters} is not the same length as {@code fieldTypes}
     */
    public IndexKeyFilter(Transaction tx, byte[] prefix, FieldType<?>[] fieldTypes, KeyFilter[] filters, int prefixLen) {
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        if (prefix == null || prefix.length == 0)
            throw new IllegalArgumentException("null/empty prefix");
        if (fieldTypes == null || fieldTypes.length == 0)
            throw new IllegalArgumentException("null/empty fieldTypes");
        if (filters == null || filters.length != fieldTypes.length)
            throw new IllegalArgumentException("bogus filters");
        if (prefixLen < 0 || prefixLen > fieldTypes.length)
            throw new IllegalArgumentException("invalid prefixLen");
        this.tx = tx;
        this.prefix = prefix;
        this.fieldTypes = fieldTypes;
        this.filters = filters.clone();
        this.prefixLen = prefixLen;
        this.prefixFilter = new FieldTypesFilter(this.prefix, this.fieldTypes, this.filters, 0, this.prefixLen);
    }

    @Override
    public String toString() {
        return "IndexKeyFilter"
          + "[prefix=" + ByteUtil.toString(this.prefix)
          + ",fieldTypes=" + Arrays.asList(this.fieldTypes)
          + ",filters=" + Arrays.asList(this.filters)
          + ",prefixLen=" + this.prefixLen
          + "]";
    }

// KeyFilter

    @Override
    public boolean contains(byte[] key) {

        // Check prefix fields
        final byte[] next = this.prefixFilter.seekHigher(key);
        if (next == null || !Arrays.equals(next, key))
            return false;

        // Do we have any suffix?
        if (this.prefixLen == this.fieldTypes.length)
            return true;

        // Determine what part of `key' constituted the prefix + prefix fields
        final ByteReader reader = new ByteReader(key, this.prefix.length);
        for (FieldType<?> fieldType : this.prefixFilter.getFieldTypes())
            fieldType.skip(reader);
        final byte[] suffixPrefix = reader.getBytes(0, reader.getOffset());

        // Search for any key with that prefix, using the suffix filter(s)
        final FieldTypesFilter suffixFilter = this.buildSuffixFilter(suffixPrefix);
        final KVPairIterator i = new KVPairIterator(this.tx.kvt, KeyRange.forPrefix(suffixPrefix), suffixFilter, false);
        if (!i.hasNext())
            return false;

        // Done
        return true;
    }

    @Override
    public byte[] seekHigher(byte[] key) {

        // Check prefix fields
        final byte[] next = this.prefixFilter.seekHigher(key);
        if (next == null || !Arrays.equals(next, key))
            return next;

        // Do we have any suffix?
        if (this.prefixLen == this.fieldTypes.length)
            return next;

        // Determine what part of `key' constituted the prefix + prefix fields
        final ByteReader reader = new ByteReader(key, this.prefix.length);
        for (FieldType<?> fieldType : this.prefixFilter.getFieldTypes())
            fieldType.skip(reader);
        final byte[] suffixPrefix = reader.getBytes(0, reader.getOffset());

        // Search for any key with that prefix, using the suffix filter(s)
        final FieldTypesFilter suffixFilter = this.buildSuffixFilter(suffixPrefix);
        final KVPairIterator i = new KVPairIterator(this.tx.kvt, KeyRange.forPrefix(suffixPrefix), suffixFilter, false);
        i.setNextTarget(key);
        if (!i.hasNext())
            return ByteUtil.getKeyAfterPrefix(suffixPrefix);

        // Done
        return i.next().getKey();
    }

    @Override
    public byte[] seekLower(byte[] key) {

        // Sanity check
        if (key == null)
            throw new IllegalArgumentException("null key");

        // Check prefix fields
        final byte[] next = this.prefixFilter.seekLower(key);
        if (next == null)
            return null;

        // Do we have any suffix?
        if (this.prefixLen == this.fieldTypes.length)
            return next;

        // Determine what part of `next' constituted the prefix + prefix fields
        final ByteReader reader = new ByteReader(key, this.prefix.length);
        try {
            for (FieldType<?> fieldType : this.prefixFilter.getFieldTypes())
                fieldType.skip(reader);
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            return next;
        }
        final byte[] suffixPrefix = reader.getBytes(0, reader.getOffset());

        // Check suffix fields
        final FieldTypesFilter suffixFilter = this.buildSuffixFilter(suffixPrefix);
        final KVPairIterator i = new KVPairIterator(this.tx.kvt, KeyRange.forPrefix(suffixPrefix), suffixFilter, true);
        i.setNextTarget(next);
        if (i.hasNext())
            return ByteUtil.getNextKey(i.next().getKey());

        // No suffix match
        return suffixPrefix;
    }

    private FieldTypesFilter buildSuffixFilter(byte[] suffixPrefix) {
        return new FieldTypesFilter(suffixPrefix, this.fieldTypes, this.filters, this.prefixLen, this.fieldTypes.length);
    }
}

