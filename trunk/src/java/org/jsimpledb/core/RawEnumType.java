
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.dellroad.stuff.java.EnumUtil;
import org.dellroad.stuff.string.ParseContext;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * Raw {@link Enum} type. Does not support null values, but generates them when a value cannot be decoded.
 */
class RawEnumType<T extends Enum<T>> extends FieldType<T> {

    private final IntegerType intType = new IntegerType();
    private final StringType stringType = new StringType();

    /**
     * Constructor.
     *
     * @throws ClassCastException if {@code type} is not an {@link Enum}
     */
    @SuppressWarnings("unchecked")
    RawEnumType(Class<T> type) {
        super((Class<T>)type.asSubclass(Enum.class));       // verify it's really an Enum
    }

// FieldType

    @Override
    public T read(ByteReader reader) {
        final int ordinal = this.intType.read(reader);
        final String name = this.stringType.read(reader);
        return this.decode(ordinal, name);
    }

    @Override
    public void copy(ByteReader reader, ByteWriter writer) {
        this.intType.copy(reader, writer);
        this.stringType.copy(reader, writer);
    }

    @Override
    public void write(ByteWriter writer, T value) {
        this.intType.write(writer, value.ordinal());
        this.stringType.write(writer, value.name());
    }

    @Override
    public byte[] getDefaultValue() {
        final byte[] intDefault = this.intType.getDefaultValue();
        final byte[] stringDefault = this.stringType.getDefaultValue();
        final byte[] result = new byte[intDefault.length + stringDefault.length];
        System.arraycopy(intDefault, 0, result, 0, intDefault.length);
        System.arraycopy(stringDefault, 0, result, intDefault.length, stringDefault.length);
        return result;
    }

    @Override
    public void skip(ByteReader reader) {
        this.intType.skip(reader);
        this.stringType.skip(reader);
    }

    @Override
    public T fromString(ParseContext ctx) {
        ctx.expect('[');
        final String name = ctx.matchPrefix("[^]]+").group();
        ctx.expect(']');
        return this.decode(null, name);
    }

    @Override
    public String toString(T value) {
        return "[" + value.name() + "]";
    }

    @Override
    public int compare(T value1, T value2) {
        return value1.compareTo(value2);
    }

    @Override
    protected boolean hasPrefix0x00() {
        return this.intType.hasPrefix0x00();
    }

    @Override
    protected boolean hasPrefix0xff() {
        return this.intType.hasPrefix0xff();
    }

    protected T decode(Integer ordinal, String name) {
        assert name != null;
        T nameValue = null;
        T ordinalValue = null;
        for (T value : EnumUtil.getValues(this.getType())) {
            if (ordinal != null && value.ordinal() == ordinal) {
                ordinalValue = value;
                if (nameValue != null)
                    break;
            }
            if (name != null && name.equals(value.name())) {
                nameValue = value;
                if (ordinalValue != null || ordinal == null)
                    break;
            }
        }
        if (nameValue != null)
            return nameValue;
        return ordinalValue;
    }

    @SuppressWarnings("unchecked")
    protected Class<T> getType() {
        return (Class<T>)this.typeToken.getRawType();
    }
}

