
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;

import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyFilterUtil;
import io.permazen.kv.KeyRanges;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;

import java.util.Arrays;
import java.util.List;

/**
 * A {@link KeyFilter} that accepts any key that is the concatenation of some prefix plus valid {@link FieldType} encoded values,
 * where each encoded value must pass a corresponding {@link KeyFilter} that is applied to just that portion of the key.
 *
 * <p>
 * Instances are immutable.
 */
class FieldTypesFilter implements KeyFilter {

    private final byte[] prefix;
    private final FieldType<?>[] fieldTypes;
    private final KeyFilter[] filters;

// Constructors

    /**
     * Construct an instance that has no per-field {@link KeyFilter}s applied yet.
     *
     * @param prefix mandatory prefix, or null for none
     * @param fieldTypes field types
     * @throws IllegalArgumentException if {@code fieldTypes} is null
     */
    FieldTypesFilter(byte[] prefix, FieldType<?>... fieldTypes) {
        Preconditions.checkArgument(fieldTypes != null, "null fieldTypes");
        this.prefix = prefix != null ? prefix.clone() : ByteUtil.EMPTY;
        this.fieldTypes = fieldTypes;
        for (FieldType<?> fieldType : this.fieldTypes)
            Preconditions.checkArgument(fieldType != null, "null fieldType");
        this.filters = new KeyFilter[this.fieldTypes.length];
    }

    FieldTypesFilter(byte[] prefix, FieldType<?>[] fieldTypes, KeyFilter[] filters, int start, int end) {
        this(prefix, Arrays.copyOfRange(fieldTypes, start, end));
        Preconditions.checkArgument(filters != null && filters.length == fieldTypes.length, "bogus filters");
        for (int i = 0; i < this.fieldTypes.length; i++)
            this.filters[i] = filters[start + i];
    }

// Private constructors

    /**
     * Copy constructor.
     */
    private FieldTypesFilter(FieldTypesFilter original) {
        this.prefix = original.prefix;
        this.fieldTypes = original.fieldTypes;
        this.filters = original.filters.clone();
    }

// Methods

    /**
     * Get the {@link FieldType}s associated with this instance.
     *
     * @return unmodifiable list of {@link FieldType}s
     */
    public List<FieldType<?>> getFieldTypes() {
        return Arrays.asList(this.fieldTypes.clone());
    }

    /**
     * Get the key filter for the {@link FieldType} at the specified index, if any.
     *
     * @return filter for the encoded field at index {@code index}, or null if no filter is applied
     * @throws IndexOutOfBoundsException if {@code index} is out of range
     */
    public KeyFilter getFilter(int index) {
        return this.filters[index];
    }

    /**
     * Determine whether any {@link FieldType}s in this instance have a filter applied.
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
     * This method works cummulatively: if this instance already has a filter for the field, the new instance filters
     * to the intersection of the existing filter and the given filter.
     *
     * @param index field index (zero-based)
     * @param keyFilter key filtering to apply
     * @throws IllegalArgumentException if {@code keyFilter} is null
     * @throws IndexOutOfBoundsException if {@code index} is out of range
     */
    public FieldTypesFilter filter(int index, KeyFilter keyFilter) {
        Preconditions.checkArgument(keyFilter != null, "null keyFilter");
        if (keyFilter instanceof KeyRanges && ((KeyRanges)keyFilter).isFull())
            return this;
        if (this.filters[index] != null)
            keyFilter = KeyFilterUtil.intersection(keyFilter, this.filters[index]);
        final FieldTypesFilter copy = new FieldTypesFilter(this);
        copy.filters[index] = keyFilter;
        return copy;
    }

    @Override
    public String toString() {
        return "FieldTypesFilter"
          + "[prefix=" + ByteUtil.toString(this.prefix)
          + ",fieldTypes=" + Arrays.asList(this.fieldTypes)
          + ",filters=" + Arrays.asList(this.filters)
          + "]";
    }

// KeyFilter

    @Override
    public boolean contains(byte[] key) {
        final byte[] next = this.seekHigher(key);
        assert next == null || ByteUtil.compare(next, key) >= 0;
        return next != null && Arrays.equals(next, key);
    }

    @Override
    public byte[] seekHigher(byte[] key) {

        // Sanity check
        Preconditions.checkArgument(key != null, "null key");

        // Check prefix
        if (!ByteUtil.isPrefixOf(this.prefix, key)) {
            if (ByteUtil.compare(key, this.prefix) > 0)
                return null;
            return this.prefix;
        }

        // Check fields
        final ByteReader reader = new ByteReader(key, this.prefix.length);
        for (int i = 0; i < this.fieldTypes.length; i++) {

            // If zero bytes are left, we have to stop at the next key (zero bytes can never be a valid encoded field value)
            final int fieldStart = reader.getOffset();
            if (reader.remain() == 0)
                return ByteUtil.getNextKey(reader.getBytes(0, fieldStart));

            // Attempt to decode next field value
            if (!this.decodeValue(this.fieldTypes[i], reader))
                return ByteUtil.getNextKey(reader.getBytes(0, reader.getOffset()));

            // Check filter, if any
            final KeyFilter filter = this.filters[i];
            if (filter == null)
                continue;

            // Get the bytes corresponding to the decoded field value
            final byte[] fieldValue = Arrays.copyOfRange(key, fieldStart, reader.getOffset());

            // Determine whether those bytes are contained in this field's filter, or if not, get lower bound on next value that is
            final byte[] next = filter.seekHigher(fieldValue);
            assert next == null || ByteUtil.compare(next, fieldValue) >= 0;

            // If field value is beyond upper limit of filter, advance to the next possible value of the previous field (if any)
            if (next == null) {
                if (i == 0)                                                     // we've gone past the range of this.prefix
                    return null;
                assert fieldStart > 0;
                try {
                    return ByteUtil.getKeyAfterPrefix(reader.getBytes(0, fieldStart));
                } catch (IllegalArgumentException e) {
                    return null;
                }
            }

            // If field's filter does not contain the field value, advance to the next possible encoded value
            if (!Arrays.equals(next, fieldValue)) {
                final byte[] keyFromNext = Bytes.concat(reader.getBytes(0, fieldStart), next);
                return ByteUtil.compare(keyFromNext, key) > 0 ? keyFromNext : ByteUtil.getNextKey(key);
            }
        }

        // All fields decoded OK - return original key
        return key;
    }

    @Override
    public byte[] seekLower(byte[] key) {

        // Sanity check
        Preconditions.checkArgument(key != null, "null key");

        // Check prefix and handle max upper bound
        boolean fromTheTop = key.length == 0;
        if (!fromTheTop && !ByteUtil.isPrefixOf(this.prefix, key)) {
            if (ByteUtil.compare(key, this.prefix) < 0)
                return null;
            fromTheTop = true;
        }
        if (fromTheTop) {
            try {
                return ByteUtil.getKeyAfterPrefix(this.prefix);
            } catch (IllegalArgumentException e) {
                return ByteUtil.EMPTY;                              // prefix is empty or all 0xff's
            }
        }

        // Check fields in order, building a concatenated upper bound. We can only proceed from one field to the next
        // when the first field is validly decoded, the corresponding filter exists, and filter.nextLower() returns
        // the same field value we decoded. Otherwise we have to stop because any smaller field value could possibly be valid.
        final ByteReader reader = new ByteReader(key, this.prefix.length);
        final ByteWriter writer = new ByteWriter(key.length);
        writer.write(key, 0, this.prefix.length);
        for (int i = 0; i < this.fieldTypes.length; i++) {
            final FieldType<?> fieldType = this.fieldTypes[i];
            final KeyFilter filter = this.filters[i];

            // Attempt to decode next field value
            final int fieldStart = reader.getOffset();
            final boolean decodeOK = this.decodeValue(fieldType, reader);
            final int fieldStop = reader.getOffset();

            // Get (partially) decoded field value
            final byte[] fieldValue = Arrays.copyOfRange(key, fieldStart, fieldStop);

            // If there is no filter (or nothing to filter), stop if decode failed, otherwise proceed
            if (filter == null || fieldValue.length == 0) {
                writer.write(fieldValue);
                if (!decodeOK)
                    break;
                assert fieldValue.length > 0;
                continue;
            }

            // Apply filter to the (partially) decoded field value; if null returned, retreat to previous field
            final byte[] next = filter.seekLower(fieldValue);
            assert next == null || fieldValue.length == 0 || ByteUtil.compare(next, fieldValue) <= 0;
            if (next == null)
                break;

            // If filter returned a strictly lower upper bound, or decode failed, we have to stop now
            if (!Arrays.equals(next, fieldValue) || !decodeOK) {
                writer.write(next);
                break;
            }

            // Filter returned same field value we gave it, so proceed to the next field
            writer.write(fieldValue);
        }

        // Done
        return writer.getBytes();
    }

// Internal methods

    private boolean decodeValue(FieldType<?> fieldType, ByteReader reader) {
        try {
            fieldType.read(reader);
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            return false;
        }
        return true;
    }
}

