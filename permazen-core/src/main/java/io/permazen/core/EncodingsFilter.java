
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.Encoding;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyFilterUtil;
import io.permazen.kv.KeyRanges;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.util.Arrays;
import java.util.List;

/**
 * A {@link KeyFilter} that accepts any key that is the concatenation of some prefix plus valid {@link Encoding} encoded values,
 * where each encoded value must pass a corresponding {@link KeyFilter} that is applied to just that portion of the key.
 *
 * <p>
 * Instances are immutable.
 */
class EncodingsFilter implements KeyFilter {

    private final ByteData prefix;
    private final Encoding<?>[] encodings;
    private final KeyFilter[] filters;

// Constructors

    /**
     * Construct an instance that has no per-field {@link KeyFilter}s applied yet.
     *
     * @param prefix mandatory prefix, or null for none
     * @param encodings encodings
     * @throws IllegalArgumentException if {@code encodings} is null
     */
    EncodingsFilter(ByteData prefix, Encoding<?>... encodings) {
        Preconditions.checkArgument(encodings != null, "null encodings");
        this.prefix = prefix != null ? prefix : ByteData.empty();
        this.encodings = encodings;
        for (Encoding<?> encoding : this.encodings)
            Preconditions.checkArgument(encoding != null, "null encoding");
        this.filters = new KeyFilter[this.encodings.length];
    }

    EncodingsFilter(ByteData prefix, Encoding<?>[] encodings, KeyFilter[] filters, int start, int end) {
        this(prefix, Arrays.copyOfRange(encodings, start, end));
        Preconditions.checkArgument(filters != null && filters.length == encodings.length, "bogus filters");
        for (int i = 0; i < this.encodings.length; i++)
            this.filters[i] = filters[start + i];
    }

// Private constructors

    /**
     * Copy constructor.
     */
    private EncodingsFilter(EncodingsFilter original) {
        this.prefix = original.prefix;
        this.encodings = original.encodings;
        this.filters = original.filters.clone();
    }

// Methods

    /**
     * Get the {@link Encoding}s associated with this instance.
     *
     * @return unmodifiable list of {@link Encoding}s
     */
    public List<Encoding<?>> getEncodings() {
        return Arrays.asList(this.encodings.clone());
    }

    /**
     * Get the key filter for the {@link Encoding} at the specified index, if any.
     *
     * @return filter for the encoded field at index {@code index}, or null if no filter is applied
     * @throws IndexOutOfBoundsException if {@code index} is out of range
     */
    public KeyFilter getFilter(int index) {
        return this.filters[index];
    }

    /**
     * Determine whether any {@link Encoding}s in this instance have a filter applied.
     */
    public boolean hasFilters() {
        for (KeyFilter filter : this.filters) {
            if (filter != null)
                return true;
        }
        return false;
    }

    /**
     * Create a new instance with the given {@link KeyFilter} applied to encoded field values at the specified index.
     * This method works cumulatively: if this instance already has a filter for the field, the new instance filters
     * to the intersection of the existing filter and the given filter.
     *
     * @param index field index (zero-based)
     * @param keyFilter key filtering to apply
     * @throws IllegalArgumentException if {@code keyFilter} is null
     * @throws IndexOutOfBoundsException if {@code index} is out of range
     */
    public EncodingsFilter filter(int index, KeyFilter keyFilter) {
        Preconditions.checkArgument(keyFilter != null, "null keyFilter");
        if (keyFilter instanceof KeyRanges && ((KeyRanges)keyFilter).isFull())
            return this;
        if (this.filters[index] != null)
            keyFilter = KeyFilterUtil.intersection(keyFilter, this.filters[index]);
        final EncodingsFilter copy = new EncodingsFilter(this);
        copy.filters[index] = keyFilter;
        return copy;
    }

    @Override
    public String toString() {
        return "EncodingsFilter"
          + "[prefix=" + ByteUtil.toString(this.prefix)
          + ",encodings=" + Arrays.asList(this.encodings)
          + ",filters=" + Arrays.asList(this.filters)
          + "]";
    }

// KeyFilter

    @Override
    public boolean contains(ByteData key) {
        final ByteData next = this.seekHigher(key);
        assert next == null || next.compareTo(key) >= 0;
        return next != null && next.equals(key);
    }

    @Override
    public ByteData seekHigher(ByteData key) {

        // Sanity check
        Preconditions.checkArgument(key != null, "null key");

        // Check prefix
        if (!key.startsWith(this.prefix)) {
            if (key.compareTo(this.prefix) > 0)
                return null;
            return this.prefix;
        }

        // Check fields
        final ByteData.Reader reader = key.newReader(this.prefix.size());
        for (int i = 0; i < this.encodings.length; i++) {

            // If zero bytes are left, we have to stop at the next key (zero bytes can never be a valid encoded field value)
            final int fieldStart = reader.getOffset();
            if (reader.remain() == 0)
                return ByteUtil.getNextKey(reader.dataReadSoFar());

            // Attempt to decode next field value
            if (!this.decodeValue(this.encodings[i], reader))
                return ByteUtil.getNextKey(reader.dataReadSoFar());

            // Check filter, if any
            final KeyFilter filter = this.filters[i];
            if (filter == null)
                continue;

            // Get the bytes corresponding to the decoded field value
            final ByteData fieldValue = key.substring(fieldStart, reader.getOffset());

            // Determine whether those bytes are contained in this field's filter, or if not, get lower bound on next value that is
            final ByteData next = filter.seekHigher(fieldValue);
            assert next == null || next.compareTo(fieldValue) >= 0;

            // If field value is beyond upper limit of filter, advance to the next possible value of the previous field (if any)
            if (next == null) {
                if (i == 0)                                                     // we've gone past the range of this.prefix
                    return null;
                assert fieldStart > 0;
                try {
                    return ByteUtil.getKeyAfterPrefix(key.substring(0, fieldStart));
                } catch (IllegalArgumentException e) {
                    return null;
                }
            }

            // If field's filter does not contain the field value, advance to the next possible encoded value
            if (!next.equals(fieldValue)) {
                final ByteData keyFromNext = key.substring(0, fieldStart).concat(next);
                return keyFromNext.compareTo(key) > 0 ? keyFromNext : ByteUtil.getNextKey(key);
            }
        }

        // All fields decoded OK - return original key
        return key;
    }

    @Override
    public ByteData seekLower(ByteData key) {

        // Sanity check
        Preconditions.checkArgument(key != null, "null key");

        // Check prefix and handle max upper bound
        boolean fromTheTop = key.isEmpty();
        if (!fromTheTop && !key.startsWith(this.prefix)) {
            if (key.compareTo(this.prefix) < 0)
                return null;
            fromTheTop = true;
        }
        if (fromTheTop) {
            try {
                return ByteUtil.getKeyAfterPrefix(this.prefix);
            } catch (IllegalArgumentException e) {
                return ByteData.empty();                            // prefix is empty or all 0xff's
            }
        }

        // Check fields in order, building a concatenated upper bound. We can only proceed from one field to the next
        // when the first field is validly decoded, the corresponding filter exists, and filter.nextLower() returns
        // the same field value we decoded. Otherwise we have to stop because any smaller field value could possibly be valid.
        final ByteData.Reader reader = key.newReader(this.prefix.size());
        final ByteData.Writer writer = ByteData.newWriter(key.size());
        writer.write(key.substring(0, this.prefix.size()));
        for (int i = 0; i < this.encodings.length; i++) {
            final Encoding<?> encoding = this.encodings[i];
            final KeyFilter filter = this.filters[i];

            // Attempt to decode next field value
            final int fieldStart = reader.getOffset();
            final boolean decodeOK = this.decodeValue(encoding, reader);
            final int fieldStop = reader.getOffset();

            // Get (partially) decoded field value
            final ByteData fieldValue = key.substring(fieldStart, fieldStop);

            // If there is no filter (or nothing to filter), stop if decode failed, otherwise proceed
            if (filter == null || fieldValue.isEmpty()) {
                writer.write(fieldValue);
                if (!decodeOK)
                    break;
                assert !fieldValue.isEmpty();
                continue;
            }

            // Apply filter to the (partially) decoded field value; if null returned, retreat to previous field
            final ByteData next = filter.seekLower(fieldValue);
            assert next == null || fieldValue.isEmpty() || next.compareTo(fieldValue) <= 0;
            if (next == null)
                break;

            // If filter returned a strictly lower upper bound, or decode failed, we have to stop now
            if (!next.equals(fieldValue) || !decodeOK) {
                writer.write(next);
                break;
            }

            // Filter returned same field value we gave it, so proceed to the next field
            writer.write(fieldValue);
        }

        // Done
        return writer.toByteData();
    }

// Internal methods

    private boolean decodeValue(Encoding<?> encoding, ByteData.Reader reader) {
        try {
            encoding.read(reader);
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            return false;
        }
        return true;
    }
}
