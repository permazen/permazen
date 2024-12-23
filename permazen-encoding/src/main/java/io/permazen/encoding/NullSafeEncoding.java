
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;

import java.util.OptionalInt;

/**
 * An {@link Encoding} that wraps any other {@link Encoding} not supporting null values and adds support for null values.
 *
 * <p>
 * This class pre-pends a {@code 0x01} to the binary encoding of non-null values, and uses a single {@code 0xff} byte to
 * represent null values. Therefore, null values sort last. Also, the default value is null.
 *
 * <p>
 * This class will automatically "inline" the {@code 0xff} for null values and omit the {@code 0x01} for non-null values
 * if the wrapped {@link Encoding}'s {@link Encoding#hasPrefix0xff} method returns false.
 *
 * @param <T> The associated Java type
 */
public class NullSafeEncoding<T> extends AbstractEncoding<T> {

    /**
     * Not null sentinel byte value. Used only when inlining is not in effect.
     */
    public static final int NOT_NULL_SENTINEL = 0x01;

    /**
     * Null sentinel byte value.
     */
    public static final int NULL_SENTINEL = 0xff;

    private static final long serialVersionUID = -6420381330755516561L;

    /**
     * The inner {@link Encoding} that this instance wraps.
     */
    protected final Encoding<T> inner;

    private final boolean inline;

    /**
     * Constructor.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param inner inner type that is not null safe
     */
    public NullSafeEncoding(EncodingId encodingId, Encoding<T> inner) {
        super(encodingId, AbstractEncoding.noNull(inner, "inner").getTypeToken().wrap(), () -> null);
        Preconditions.checkArgument(!inner.supportsNull(), "inner type is already null-safe");
        this.inner = inner;
        this.inline = !inner.hasPrefix0xff();
    }

    /**
     * Get the inner {@link Encoding} that this instance wraps.
     *
     * @return inner type that is not null safe
     */
    public Encoding<T> getInnerEncoding() {
        return this.inner;
    }

// Encoding

    @Override
    public T read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        if (this.inline) {
            if (reader.peek() == NULL_SENTINEL) {
                reader.skip(1);
                return null;
            }
            return this.inner.read(reader);
        } else {
            switch (reader.readByte()) {
            case NULL_SENTINEL:
                return null;
            case NOT_NULL_SENTINEL:
                return this.inner.read(reader);
            default:
                throw new IllegalArgumentException(String.format("invalid encoding of %s", this));
            }
        }
    }

    @Override
    public void write(ByteData.Writer writer, T value) {
        Preconditions.checkArgument(writer != null);
        if (value == null) {
            writer.write(NULL_SENTINEL);
            return;
        }
        if (!this.inline)
            writer.write(NOT_NULL_SENTINEL);
        this.inner.write(writer, value);
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        if (this.inline) {
            final int prefix = reader.peek();
            switch (prefix) {
            case NULL_SENTINEL:
                reader.skip(1);
                break;
            default:
                this.inner.skip(reader);
                break;
            }
        } else {
            final int prefix = reader.readByte();
            switch (prefix) {
            case NULL_SENTINEL:
                break;
            case NOT_NULL_SENTINEL:
                this.inner.skip(reader);
                break;
            default:
                throw new IllegalArgumentException(String.format("invalid encoding of %s", this));
            }
        }
    }

    @Override
    public T fromString(String string) {
        return this.inner.fromString(string);
    }

    @Override
    public String toString(T value) {
        Preconditions.checkArgument(value != null, "null value");
        return this.inner.toString(value);
    }

    @Override
    public <S> T convert(Encoding<S> type, S value) {
        Preconditions.checkArgument(type != null, "null type");
        return value == null ? null : this.inner.convert(type, value);
    }

    @Override
    public int compare(T value1, T value2) {
        if (value1 == null)
            return value2 == null ? 0 : 1;
        if (value2 == null)
            return -1;
        return this.inner.compare(value1, value2);
    }

    @Override
    public boolean sortsNaturally() {
        return inner.sortsNaturally();
    }

    /**
     * Always returns true.
     *
     * @return true
     */
    @Override
    public boolean supportsNull() {
        return true;
    }

    /**
     * Always returns true.
     *
     * @return true
     */
    @Override
    public boolean hasPrefix0xff() {
        return true;
    }

    @Override
    public boolean hasPrefix0x00() {
        return this.inline && this.inner.hasPrefix0x00();
    }

    @Override
    public OptionalInt getFixedWidth() {
        final int requiredWidth = this.inline ? 1 : 0;
        final OptionalInt width = this.inner.getFixedWidth();
        return width.isPresent() && width.getAsInt() == requiredWidth ? OptionalInt.of(1) : OptionalInt.empty();
    }

// Object

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.inner.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final NullSafeEncoding<?> that = (NullSafeEncoding<?>)obj;
        return this.inner.equals(that.inner);
    }
}
