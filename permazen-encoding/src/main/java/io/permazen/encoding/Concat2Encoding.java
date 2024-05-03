
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

/**
 * Support superclass for {@link Encoding}s of values that can be decomposed into two component values,
 * with the combined encoding formed by concatenating the two component {@link Encoding}s.
 *
 * @param <T> this encoding's target type
 * @param <S1> first component encoding type
 * @param <S2> second component encoding type
 */
public abstract class Concat2Encoding<T, S1, S2> extends AbstractEncoding<T> {

    private static final long serialVersionUID = -7395218884659436172L;

    protected final Encoding<S1> encoding1;
    protected final Encoding<S2> encoding2;

    /**
     * Constructor.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param typeToken Java type for this encoding's values
     * @param defaultValue default value for this encoding; must be null if this encoding supports nulls
     * @param encoding1 first encoding
     * @param encoding2 second encoding
     * @throws IllegalArgumentException if {@code typeToken}, {@code encoding1}, or {@code encoding2} is null
     */
    protected Concat2Encoding(EncodingId encodingId, TypeToken<T> typeToken,
      T defaultValue, Encoding<S1> encoding1, Encoding<S2> encoding2) {
        super(encodingId, typeToken, defaultValue);
        Preconditions.checkArgument(encoding1 != null, "null encoding1");
        Preconditions.checkArgument(encoding2 != null, "null encoding2");
        this.encoding1 = encoding1;
        this.encoding2 = encoding2;
    }

    /**
     * Constructor.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param type Java type for this encoding's values
     * @param defaultValue default value for this encoding; must be null if this encoding supports nulls
     * @param encoding1 first encoding
     * @param encoding2 second encoding
     * @throws IllegalArgumentException if {@code type}, {@code encoding1}, or {@code encoding2} is null
     */
    protected Concat2Encoding(EncodingId encodingId, Class<T> type,
      T defaultValue, Encoding<S1> encoding1, Encoding<S2> encoding2) {
        this(encodingId, TypeToken.of(AbstractEncoding.noNull(type, "type")), defaultValue, encoding1, encoding2);
    }

// Encoding

    @Override
    public T read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return this.join(this.encoding1.read(reader), this.encoding2.read(reader));
    }

    @Override
    public void write(ByteWriter writer, T value) {
        Preconditions.checkArgument(value != null, "null value");
        Preconditions.checkArgument(writer != null);
        this.encoding1.write(writer, this.split1(value));
        this.encoding2.write(writer, this.split2(value));
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        this.encoding1.skip(reader);
        this.encoding2.skip(reader);
    }

    @Override
    public int compare(T value1, T value2) {
        int diff = this.encoding1.compare(this.split1(value1), this.split1(value2));
        if (diff != 0)
            return diff;
        return this.encoding2.compare(this.split2(value1), this.split2(value2));
    }

    @Override
    public boolean hasPrefix0x00() {
        return this.encoding1.hasPrefix0x00();
    }

    @Override
    public boolean hasPrefix0xff() {
        return this.encoding1.hasPrefix0xff();
    }

    /**
     * Combine two component values to create a new combined value.
     *
     * @param value1 first component value
     * @param value2 second component value
     * @return new combined value
     */
    protected abstract T join(S1 value1, S2 value2);

    /**
     * Extract the first component value from the given combined value.
     *
     * @param value combined value
     * @return first component value
     */
    protected abstract S1 split1(T value);

    /**
     * Extract the second component value from the given combined value.
     *
     * @param value combined value
     * @return second component value
     */
    protected abstract S2 split2(T value);
}
