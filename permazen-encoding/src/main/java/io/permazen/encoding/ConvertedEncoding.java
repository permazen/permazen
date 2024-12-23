
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteData;

import java.util.OptionalInt;
import java.util.function.Supplier;

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
 * This encoding has a default value when the delegate has one, namely, the conversion of the delegate's default value.
 *
 * <p>
 * By default, the {@link String} form of a value is just the {@link String} form of its conversion. Subclasses may
 * therefore want to override {@link #toString(T) toString()} and {@link #fromString fromString()}.
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

    protected final Encoding<S> delegate;
    @SuppressWarnings("serial")
    protected final Converter<T, S> converter;

    private final boolean sortsNaturally;

// Constructors

    /**
     * Primary constructor.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param typeToken represented Java type
     * @param delegate delegate encoder
     * @param converter value converter
     * @param sortsNaturally true if this encoding {@linkplain Encoding#sortsNaturally sorts naturally}, otherwise false
     * @throws IllegalArgumentException if any parameter other than {@code defaultValueSupplier} is null
     */
    public ConvertedEncoding(EncodingId encodingId, TypeToken<T> typeToken,
      Encoding<S> delegate, Converter<T, S> converter, boolean sortsNaturally) {
        super(encodingId, typeToken, ConvertedEncoding.convertedDefault(delegate, converter));
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
     * @param delegate delegate encoder
     * @param converter converts between native form and {@link String} form; should be {@link java.io.Serializable}
     * @param sortsNaturally true if this encoding {@linkplain Encoding#sortsNaturally sorts naturally}, otherwise false
     * @throws IllegalArgumentException if any parameter other than {@code defaultValueSupplier} is null
     */
    public ConvertedEncoding(EncodingId encodingId, Class<T> type,
      Encoding<S> delegate, Converter<T, S> converter, boolean sortsNaturally) {
        this(encodingId, TypeToken.of(AbstractEncoding.noNull(type, "type")), delegate, converter, sortsNaturally);
    }

    /**
     * Convenience constructor for when the new {@link ConvertedEncoding} does not sort naturally.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param type represented Java type
     * @param delegate delegate encoder
     * @param converter value converter
     * @throws IllegalArgumentException if any parameter is null
     */
    public ConvertedEncoding(EncodingId encodingId, Class<T> type, Encoding<S> delegate, Converter<T, S> converter) {
        this(encodingId, type, delegate, converter, false);
    }

    private static <T, S> Supplier<? extends T> convertedDefault(Encoding<S> delegate, Converter<T, S> converter) {
        Preconditions.checkArgument(delegate != null, "null delegate");
        Preconditions.checkArgument(converter != null, "null converter");
        final S defaultValue;
        try {
            defaultValue = delegate.getDefaultValue();
        } catch (UnsupportedOperationException e) {
            return null;
        }
        final Converter<S, T> reverseConverter = converter.reverse();
        return () -> reverseConverter.convert(defaultValue);
    }

// Encoding

    @Override
    public T read(ByteData.Reader reader) {
        return this.converter.reverse().convert(this.delegate.read(reader));
    }

    @Override
    public void write(ByteData.Writer writer, T obj) {
        this.delegate.write(writer, this.converter.convert(obj));
    }

    @Override
    public void skip(ByteData.Reader reader) {
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
    public final OptionalInt getFixedWidth() {
        return this.delegate.getFixedWidth();
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

// Object

    @Override
    public int hashCode() {
        return super.hashCode()
          ^ this.delegate.hashCode()
          ^ this.converter.hashCode()
          ^ Boolean.hashCode(this.sortsNaturally);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ConvertedEncoding<?, ?> that = (ConvertedEncoding<?, ?>)obj;
        return this.delegate.equals(that.delegate)
          && this.converter.equals(that.converter)
          && this.sortsNaturally == that.sortsNaturally;
    }
}
