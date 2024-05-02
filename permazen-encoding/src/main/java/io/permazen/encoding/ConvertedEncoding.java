
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

/**
 * An {@link Encoding} that relies on some other {@link Encoding}, converting values to/from
 * the other {@link Encoding}'s Java type.
 *
 * <p>
 * This class provides a convenient way to implement custom {@link Encoding}s when the target values can be converted
 * into some other form for which another {@link Encoding} (the "delegate") already exists. A Guava {@link Converter}
 * converts values between the type supported by this encoding and the type supported by the delegate encoding.
 * It's {@link Converter#convert convert()} method must throw {@link IllegalArgumentException} if an unsupported
 * value is encountered. The {@link Converter} should be <i>non-lossy</i> when supported values are round-tripped.
 *
 * <p>
 * This encoding will sort values in the same order as the delegate sorts the corresponding converted values.
 * Null values are supported by this encoding when the delegate supports them; if so, null always converts to/from null.
 *
 * <p>
 * The {@link Converter} must be {@link java.io.Serializable} in order for an instance of this class to also be
 * {@link java.io.Serializable}.
 *
 * @param <T> This encoder's value type
 * @param <S> The delegate encoder's value type
 */
public class ConvertedEncoding<T, S> extends AbstractEncoding<T> {

    private static final long serialVersionUID = -1873774754387478399L;

    private final Encoding<S> delegate;
    @SuppressWarnings("serial")
    private final Converter<T, S> converter;
    private final boolean sortsNaturally;

// Constructors

    /**
     * Primary constructor.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param typeToken represented Java type
     * @param defaultValue default value for this encoding; must be null if this encoding supports nulls
     * @param delegate delegate encoder
     * @param converter value converter
     * @param sortsNaturally true if this encoding {@linkplain Encoding#sortsNaturally sorts naturally}, otherwise false
     * @throws IllegalArgumentException if any parameter other than {@code defaultValue} is null
     */
    public ConvertedEncoding(EncodingId encodingId, TypeToken<T> typeToken, T defaultValue,
      Encoding<S> delegate, Converter<T, S> converter, boolean sortsNaturally) {
        super(encodingId, typeToken, defaultValue);
        Preconditions.checkArgument(delegate != null, "null delegate");
        Preconditions.checkArgument(converter != null, "null converter");
        Preconditions.checkArgument(converter.convert(null) == null && converter.reverse().convert(null) == null,
          "converter does not convert null to null");
        this.delegate = delegate;
        this.converter = converter;
        this.sortsNaturally = sortsNaturally;
    }

    /**
     * Convenience constructor taking {@link Class} instead of {@link TypeToken}.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param type represented Java type
     * @param defaultValue default value for this encoding; must be null if this encoding supports nulls
     * @param delegate delegate encoder
     * @param converter converts between native form and {@link String} form; should be {@link java.io.Serializable}
     * @param sortsNaturally true if this encoding {@linkplain Encoding#sortsNaturally sorts naturally}, otherwise false
     * @throws IllegalArgumentException if any parameter other than {@code defaultValue} is null
     */
    public ConvertedEncoding(EncodingId encodingId, Class<T> type, T defaultValue,
      Encoding<S> delegate, Converter<T, S> converter, boolean sortsNaturally) {
        this(encodingId, TypeToken.of(AbstractEncoding.noNull(type, "type")), defaultValue, delegate, converter, sortsNaturally);
    }

// Encoding

    @Override
    public ConvertedEncoding<T, S> withEncodingId(EncodingId encodingId) {
        return new ConvertedEncoding<>(encodingId, this.typeToken,
          this.getDefaultValue(), this.delegate, this.converter, this.sortsNaturally);
    }

    @Override
    public T read(ByteReader reader) {
        return this.converter.reverse().convert(this.delegate.read(reader));
    }

    @Override
    public void write(ByteWriter writer, T obj) {
        this.delegate.write(writer, this.converter.convert(obj));
    }

    @Override
    public void skip(ByteReader reader) {
        this.delegate.skip(reader);
    }

    @Override
    public String toString(T obj) {
        Preconditions.checkArgument(obj != null, "null value");
        return this.delegate.toString(this.converter.convert(obj));
    }

    @Override
    public T fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return this.converter.reverse().convert(this.delegate.fromString(string));
    }

    @Override
    public boolean sortsNaturally() {
        return this.sortsNaturally;
    }

    @Override
    public final boolean supportsNull() {
        return this.delegate.supportsNull();
    }

    @Override
    public T validate(Object obj) {
        final T value = super.validate(obj);
        this.delegate.validate(this.converter.convert(value));
        return value;
    }

    @Override
    public int compare(T obj1, T obj2) {
        return this.delegate.compare(this.converter.convert(obj1), this.converter.convert(obj2));
    }

    @Override
    public boolean hasPrefix0x00() {
        return this.delegate.hasPrefix0x00();
    }

    @Override
    public boolean hasPrefix0xff() {
        return this.delegate.hasPrefix0xff();
    }
}
