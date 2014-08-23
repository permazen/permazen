
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Raw {@link Enum} type. Does not support null values.
 *
 * <p>
 * Instances sort by ordinal value, then name.
 * </p>
 */
class EnumValueType extends FieldType<EnumValue> {

    private final StringType stringType = new StringType();

    EnumValueType() {
        super(EnumValue.class);
    }

// FieldType

    @Override
    public EnumValue read(ByteReader reader) {
        final int ordinal = UnsignedIntEncoder.read(reader);
        final String name = this.stringType.read(reader);
        return new EnumValue(name, ordinal);
    }

    @Override
    public void write(ByteWriter writer, EnumValue value) {
        UnsignedIntEncoder.write(writer, value.getOrdinal());
        this.stringType.write(writer, value.getName());
    }

    @Override
    public byte[] getDefaultValue() {
        throw new UnsupportedOperationException();          // null is the default value
    }

    @Override
    public void skip(ByteReader reader) {
        UnsignedIntEncoder.skip(reader);
        this.stringType.skip(reader);
    }

    @Override
    public EnumValue fromParseableString(ParseContext ctx) {
        final String name = ctx.matchPrefix("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*").group();
        final int ordinal = ctx.tryLiteral("#") ? FieldType.INTEGER.fromParseableString(ctx) : -1;
        return new EnumValue(name, ordinal);
    }

    @Override
    public String toParseableString(EnumValue value) {
        return value.toString();
    }

    @Override
    public int compare(EnumValue value1, EnumValue value2) {
        int diff = Integer.compare(value1.getOrdinal(), value2.getOrdinal());
        if (diff != 0)
            return diff;
        diff = value1.getName().compareTo(value2.getName());
        if (diff != 0)
            return diff;
        return 0;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }
}

