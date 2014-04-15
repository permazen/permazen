
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

import org.dellroad.stuff.string.ParseContext;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * Non-null type for fields that contain a reference to an object. Null values are not supported by this class.
 */
public class ReferenceType extends FieldType<ObjId> {

    public static final String NAME = "reference";

    ReferenceType() {
        super(NAME, TypeToken.of(ObjId.class));
    }

// FieldType

    @Override
    public ObjId read(ByteReader reader) {
        return new ObjId(reader);
    }

    @Override
    public void copy(ByteReader reader, ByteWriter writer) {
        writer.write(reader.readBytes(ObjId.NUM_BYTES));
    }

    @Override
    public void write(ByteWriter writer, ObjId id) {
        writer.write(id.getBytes());
    }

    @Override
    public byte[] getDefaultValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skip(ByteReader reader) {
        reader.skip(ObjId.NUM_BYTES);
    }

    @Override
    public ObjId fromString(ParseContext ctx) {
        return new ObjId(ctx.matchPrefix(ObjId.PATTERN).group());
    }

    @Override
    public String toString(ObjId value) {
        return value.toString();
    }

    @Override
    public int compare(ObjId id1, ObjId id2) {
        return id1.compareTo(id2);
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public boolean hasPrefix0x00() {
        return false;                       // ObjId's may not have a storage ID of zero
    }
}

