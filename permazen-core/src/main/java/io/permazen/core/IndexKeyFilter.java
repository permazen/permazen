
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPairIterator;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;

import java.util.Arrays;

/**
 * A {@link KeyFilter} based on an {@link AbstractIndexView}. A prefix of the fields is used to filter the key directly,
 * and any remaining fields are used to further filter the key based on the existence of any combined keys (prefix + suffix)
 * in the underlying {@link Transaction}.
 */
class IndexKeyFilter implements KeyFilter {

    private final KVStore kv;
    private final byte[] prefix;                                // prefix to always skip over
    private final Encoding<?>[] encodings;                    // fields to decode after prefix
    private final KeyFilter[] filters;                          // filters to apply to fields
    private final int prefixLen;                                // how many fields are mandatory

    private final EncodingsFilter prefixFilter;

    /**
     * Constructor from an index view.
     *
     * @param kv key/value data
     * @param indexView associated index view
     * @param prefixLen the number of fields in {@code indexView} that are considered mandatory
     * @throws IllegalArgumentException if {@code kv} or {@code or indexView} is null
     * @throws IllegalArgumentException if {@code prefixLen} is zero or out of range
     */
    IndexKeyFilter(KVStore kv, AbstractIndexView indexView, int prefixLen) {
        this(kv, indexView.prefix, indexView.encodings, indexView.filters, prefixLen);
    }

    /**
     * Primary constructor.
     *
     * @param kv key/value data
     * @param prefix prefix before first encoding
     * @param encodings one or more index encodings
     * @param filters filters corresponding to {@code encodings}
     * @param prefixLen the number of fields in {@code encodings} that are considered mandatory
     * @throws IllegalArgumentException if {@code kv} is null
     * @throws IllegalArgumentException if {@code encodings} is null or empty
     * @throws IllegalArgumentException if {@code prefix} is null or empty
     * @throws IllegalArgumentException if {@code prefixLen} is zero or out of range
     * @throws IllegalArgumentException if {@code filters} is not the same length as {@code encodings}
     */
    IndexKeyFilter(KVStore kv, byte[] prefix, Encoding<?>[] encodings, KeyFilter[] filters, int prefixLen) {
        Preconditions.checkArgument(kv != null, "null kv");
        Preconditions.checkArgument(prefix != null && prefix.length > 0, "null/empty prefix");
        Preconditions.checkArgument(encodings != null && encodings.length > 0, "null/empty encodings");
        Preconditions.checkArgument(filters != null && filters.length == encodings.length, "bogus filters");
        Preconditions.checkArgument(prefixLen >= 0 && prefixLen <= encodings.length, "invalid prefixLen");
        this.kv = kv;
        this.prefix = prefix;
        this.encodings = encodings;
        this.filters = filters.clone();
        this.prefixLen = prefixLen;
        this.prefixFilter = new EncodingsFilter(this.prefix, this.encodings, this.filters, 0, this.prefixLen);
    }

    @Override
    public String toString() {
        return "IndexKeyFilter"
          + "[prefix=" + ByteUtil.toString(this.prefix)
          + ",encodings=" + Arrays.asList(this.encodings)
          + ",filters=" + Arrays.asList(this.filters)
          + ",prefixLen=" + this.prefixLen
          + "]";
    }

// KeyFilter

    @Override
    public boolean contains(byte[] key) {

        // Check prefix fields
        final byte[] next = this.prefixFilter.seekHigher(key);
        assert next == null || ByteUtil.compare(next, key) >= 0;
        if (next == null || !Arrays.equals(next, key))
            return false;

        // Do we have any suffix?
        if (this.prefixLen == this.encodings.length)
            return true;

        // Determine what part of `key' constituted the prefix + prefix fields
        final ByteReader reader = new ByteReader(key, this.prefix.length);
        for (Encoding<?> encoding : this.prefixFilter.getEncodings())
            encoding.skip(reader);
        final byte[] suffixPrefix = reader.getBytes(0, reader.getOffset());

        // Search for any key with that prefix, using the suffix filter(s)
        final EncodingsFilter suffixFilter = this.buildSuffixFilter(suffixPrefix);
        try (KVPairIterator i = new KVPairIterator(this.kv, KeyRange.forPrefix(suffixPrefix), suffixFilter, false)) {
            return i.hasNext();
        }
    }

    @Override
    public byte[] seekHigher(byte[] key) {

        // Check prefix fields
        final byte[] next = this.prefixFilter.seekHigher(key);
        assert next == null || ByteUtil.compare(next, key) >= 0;
        if (next == null || !Arrays.equals(next, key))
            return next;

        // Do we have any suffix?
        if (this.prefixLen == this.encodings.length)
            return next;

        // Determine what part of `key' constituted the prefix + prefix fields
        final ByteReader reader = new ByteReader(key, this.prefix.length);
        for (Encoding<?> encoding : this.prefixFilter.getEncodings())
            encoding.skip(reader);
        final byte[] suffixPrefix = reader.getBytes(0, reader.getOffset());

        // Search for any key with that prefix, using the suffix filter(s)
        final EncodingsFilter suffixFilter = this.buildSuffixFilter(suffixPrefix);
        try (KVPairIterator i = new KVPairIterator(this.kv, KeyRange.forPrefix(suffixPrefix), suffixFilter, false)) {
            i.setNextTarget(key);
            return i.hasNext() ? i.next().getKey() : ByteUtil.getKeyAfterPrefix(suffixPrefix);
        }
    }

    @Override
    public byte[] seekLower(byte[] key) {

        // Sanity check
        Preconditions.checkArgument(key != null, "null key");

        // Check prefix fields
        final byte[] next = this.prefixFilter.seekLower(key);
        assert next == null || key.length == 0 || ByteUtil.compare(next, key) <= 0;
        if (next == null)
            return null;

        // Do we have any suffix?
        if (this.prefixLen == this.encodings.length)
            return next;

        // Determine what part of `next' constituted the prefix + prefix fields
        final ByteReader reader = new ByteReader(key, this.prefix.length);
        try {
            for (Encoding<?> encoding : this.prefixFilter.getEncodings())
                encoding.skip(reader);
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            return next;
        }
        final byte[] suffixPrefix = reader.getBytes(0, reader.getOffset());

        // Check suffix fields
        final EncodingsFilter suffixFilter = this.buildSuffixFilter(suffixPrefix);
        try (KVPairIterator i = new KVPairIterator(this.kv, KeyRange.forPrefix(suffixPrefix), suffixFilter, true)) {
            i.setNextTarget(next);
            return i.hasNext() ? ByteUtil.getNextKey(i.next().getKey()) : suffixPrefix;
        }
    }

    private EncodingsFilter buildSuffixFilter(byte[] suffixPrefix) {
        return new EncodingsFilter(suffixPrefix, this.encodings, this.filters, this.prefixLen, this.encodings.length);
    }
}
