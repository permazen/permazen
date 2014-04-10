
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.dellroad.stuff.string.ParseContext;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * A {@link FieldType} that wraps any other {@link FieldType} not supporting null values and adds support for null values
 * by pre-pending to the binary encoding a {@code 0x00} for non-null values or a {@code 0xff} byte for null values. Therefore,
 * null values sort last.
 *
 * <p>
 * The default value becomes null and {@code "null"} is an accepted string value (therefore, {@code "null"} must not be
 * a valid string value for the wrapped type).
 * </p>
 *
 * <p>
 * This class will automatically "inline" the {@code 0xff} for null values if the wrapped {@link FieldType}'s
 * {@link FieldType#hasPrefix0xff} returns false.
 * </p>
 */
public class NullSafeType<T> extends FieldType<T> {

    /**
     * Not null sentinel byte value. Used only when inlining is not in effect.
     */
    public static final int NOT_NULL_SENTINEL = 0x01;

    /**
     * Null sentinel byte value.
     */
    public static final int NULL_SENTINEL = 0xff;

    private static final byte[] DEFAULT_VALUE = new byte[] { (byte)NULL_SENTINEL };

    private final FieldType<T> inner;
    private final boolean inline;

    /**
     * Constructor.
     *
     * @param name type name
     * @param inner inner type that is not null safe
     */
    public NullSafeType(String name, FieldType<T> inner) {
       super(name, inner.getTypeToken().wrap());
       this.inner = inner;
       this.inline = !inner.hasPrefix0xff();
    }

    /**
     * Constructor. Takes type name from {@code inner}; therefore, this instance and {@code inner}
     * cannot be both registered in a {@link FieldTypeRegistry}.
     *
     * @param inner inner type that is not null safe
     */
    public NullSafeType(FieldType<T> inner) {
       this(inner.name, inner);
    }

    @Override
    public T read(ByteReader reader) {
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
        if (value == null) {
            writer.writeByte(NULL_SENTINEL);
            return;
        }
        if (!this.inline)
            writer.writeByte(NOT_NULL_SENTINEL);
        this.inner.write(writer, value);
    }

    @Override
    public void copy(ByteReader reader, ByteWriter writer) {
        if (this.inline) {
            final int prefix = reader.peek();
            switch (prefix) {
            case NULL_SENTINEL:
                writer.writeByte(prefix);
                break;
            default:
                this.inner.copy(reader, writer);
                break;
            }
        } else {
            final int prefix = reader.readByte();
            writer.writeByte(prefix);
            switch (prefix) {
            case NULL_SENTINEL:
                break;
            case NOT_NULL_SENTINEL:
                this.inner.copy(reader, writer);
                break;
            default:
                throw new IllegalArgumentException("invalid encoding of " + this);
            }
        }
    }

    @Override
    public byte[] getDefaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public void skip(ByteReader reader) {
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
    public T fromString(ParseContext context) {
        return context.tryLiteral("null") ? null : this.inner.fromString(context);
    }

    @Override
    public String toString(T value) {
        return value == null ? "null" : this.inner.toString(value);
    }

    @Override
    public int compare(T value1, T value2) {
        if (value1 == null)
            return value2 == null ? 0 : 1;
        if (value2 == null)
            return value1 == null ? 0 : -1;
        return this.inner.compare(value1, value2);
    }

    @Override
    protected boolean hasPrefix0xff() {
        return true;
    }

    @Override
    protected boolean hasPrefix0x00() {
        return this.inline && this.inner.hasPrefix0x00();
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

