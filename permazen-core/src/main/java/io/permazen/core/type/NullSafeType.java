
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;

import io.permazen.core.AbstractFieldType;
import io.permazen.core.EncodingId;
import io.permazen.core.FieldType;
import io.permazen.core.FieldTypeRegistry;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.ParseContext;

/**
 * A {@link FieldType} that wraps any other {@link FieldType} not supporting null values and adds support for null values.
 *
 * <p>
 * This class pre-pends a {@code 0x01} to the binary encoding of non-null values, and uses a single {@code 0xff} byte to
 * represent null values. Therefore, null values sort last.
 *
 * <p>
 * The default value becomes null, for which {@code "null"} is the value returned by {@link #toParseableString toParseableString()}.
 * Therefore, {@code "null"} must not be returned by the wrapped type's {@link #toParseableString toParseableString()}
 * for any value.
 *
 * <p>
 * This class will automatically "inline" the {@code 0xff} for null values and omit the {@code 0x01} for non-null values
 * if the wrapped {@link FieldType}'s {@link FieldType#hasPrefix0xff} method returns false.
 *
 * @param <T> The associated Java type
 */
public class NullSafeType<T> extends AbstractFieldType<T> {

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
     * The inner {@link FieldType} that this instance wraps.
     */
    protected final FieldType<T> inner;

    private final boolean inline;

    /**
     * Constructor.
     *
     * @param encodingId encoding ID
     * @param inner inner type that is not null safe
     */
    public NullSafeType(EncodingId encodingId, FieldType<T> inner) {
        super(encodingId, inner.getTypeToken().wrap(), null);
        Preconditions.checkArgument(!(inner instanceof NullSafeType), "inner type is already null-safe");
        this.inner = inner;
        this.inline = !inner.hasPrefix0xff();
    }

    /**
     * Constructor when wrapping a non-registered inner type.
     *
     * <p>
     * Takes encoding ID from {@code inner}; therefore, this instance and {@code inner}
     * cannot be both registered in a {@link FieldTypeRegistry}.
     *
     * @param inner inner type that is not null safe
     */
    public NullSafeType(FieldType<T> inner) {
       this(inner.getEncodingId(), inner);
    }

    /**
     * Get the inner {@link FieldType} that this instance wraps.
     *
     * @return inner type that is not null safe
     */
    public FieldType<T> getInnerType() {
        return this.inner;
    }

    @Override
    public T read(ByteReader reader) {
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
                throw new IllegalArgumentException("invalid encoding of " + this);
            }
        }
    }

    @Override
    public void write(ByteWriter writer, T value) {
        Preconditions.checkArgument(writer != null);
        if (value == null) {
            writer.writeByte(NULL_SENTINEL);
            return;
        }
        if (!this.inline)
            writer.writeByte(NOT_NULL_SENTINEL);
        this.inner.write(writer, value);
    }

    @Override
    public void skip(ByteReader reader) {
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
                throw new IllegalArgumentException("invalid encoding of " + this);
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
    public T fromParseableString(ParseContext context) {
        return context.tryPattern("null\\b") != null ? null : this.inner.fromParseableString(context);
    }

    @Override
    public String toParseableString(T value) {
        return value == null ? "null" : this.inner.toParseableString(value);
    }

    @Override
    public <S> T convert(FieldType<S> type, S value) {
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
    public boolean hasPrefix0xff() {
        return true;
    }

    @Override
    public boolean hasPrefix0x00() {
        return this.inline && this.inner.hasPrefix0x00();
    }

    @Override
    public NullSafeType<T> genericizeForIndex() {
        return new NullSafeType<>(this.inner.genericizeForIndex());
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
        final NullSafeType<?> that = (NullSafeType<?>)obj;
        return this.inner.equals(that.inner);
    }
}
