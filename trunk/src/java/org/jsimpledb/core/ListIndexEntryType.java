
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * {@link FieldType} of a list index entry.
 */
class ListIndexEntryType extends FieldType<ListIndexEntry> {

    ListIndexEntryType() {
        super("ListIndexEntry", TypeToken.of(ListIndexEntry.class));
    }

    @Override
    public ListIndexEntry read(ByteReader reader) {
        final ObjId id = FieldTypeRegistry.OBJ_ID.read(reader);
        final int index = UnsignedIntEncoder.read(reader);
        return new ListIndexEntry(id, index);
    }

    @Override
    public void write(ByteWriter writer, ListIndexEntry entry) {
        if (entry == null)
            throw new IllegalArgumentException("null entry");
        FieldTypeRegistry.OBJ_ID.write(writer, entry.getObjId());
        UnsignedIntEncoder.write(writer, entry.getIndex());
    }

    @Override
    public byte[] getDefaultValue() {
        final byte[] b1 = FieldTypeRegistry.OBJ_ID.getDefaultValue();
        final byte[] b2 = UnsignedIntEncoder.encode(0);
        final byte[] result = new byte[b1.length + b2.length];
        System.arraycopy(b1, 0, result, 0, b1.length);
        System.arraycopy(b2, 0, result, b1.length, b2.length);
        return result;
    }

    @Override
    public void skip(ByteReader reader) {
        FieldTypeRegistry.OBJ_ID.skip(reader);
        UnsignedIntEncoder.skip(reader);
    }

    @Override
    public ListIndexEntry validate(Object obj) {
        if (obj == null)
            throw new IllegalArgumentException("illegal null value");
        return super.validate(obj);
    }

    @Override
    public String toParseableString(ListIndexEntry entry) {
        if (entry == null)
            throw new IllegalArgumentException("null entry");
        return "[" + FieldTypeRegistry.OBJ_ID.toParseableString(entry.getObjId()) + "," + entry.index + "]";
    }

    @Override
    public int compare(ListIndexEntry entry1, ListIndexEntry entry2) {
        if (entry1 == null || entry2 == null)
            throw new IllegalArgumentException("null entry");
        int diff = FieldTypeRegistry.OBJ_ID.compare(entry1.id, entry2.id);
        if (diff != 0)
            return diff;
        diff = Integer.compare(entry1.index, entry2.index);
        if (diff != 0)
            return diff;
        return 0;
    }

    @Override
    public ListIndexEntry fromParseableString(ParseContext context) {
        context.expect('[');
        final ObjId id = FieldTypeRegistry.OBJ_ID.fromParseableString(context);
        context.expect(',');
        final int index = FieldTypeRegistry.INTEGER.fromParseableString(context);
        context.expect(']');
        return new ListIndexEntry(id, index);
    }

    @Override
    public boolean hasPrefix0xff() {
        return FieldTypeRegistry.OBJ_ID.hasPrefix0xff();
    }

    @Override
    public boolean hasPrefix0x00() {
        return FieldTypeRegistry.OBJ_ID.hasPrefix0x00();
    }
}

