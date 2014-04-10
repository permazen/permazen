
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import org.dellroad.stuff.string.ParseContext;
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
        final ObjId id = FieldType.REFERENCE.read(reader);
        final int index = UnsignedIntEncoder.read(reader);
        return new ListIndexEntry(id, index);
    }

    @Override
    public void copy(ByteReader reader, ByteWriter writer) {
        FieldType.REFERENCE.copy(reader, writer);
        writer.write(reader.readBytes(UnsignedIntEncoder.decodeLength(reader.peek())));
    }

    @Override
    public void write(ByteWriter writer, ListIndexEntry entry) {
        if (entry == null)
            throw new IllegalArgumentException("null entry");
        FieldType.REFERENCE.write(writer, entry.getObjId());
        UnsignedIntEncoder.write(writer, entry.getIndex());
    }

    @Override
    public byte[] getDefaultValue() {
        final byte[] b1 = FieldType.REFERENCE.getDefaultValue();
        final byte[] b2 = UnsignedIntEncoder.encode(0);
        final byte[] result = new byte[b1.length + b2.length];
        System.arraycopy(b1, 0, result, 0, b1.length);
        System.arraycopy(b2, 0, result, b1.length, b2.length);
        return result;
    }

    @Override
    public void skip(ByteReader reader) {
        FieldType.REFERENCE.skip(reader);
        UnsignedIntEncoder.skip(reader);
    }

    @Override
    public ListIndexEntry validate(Object obj) {
        if (obj == null)
            throw new IllegalArgumentException("illegal null value");
        return super.validate(obj);
    }

    @Override
    public String toString(ListIndexEntry entry) {
        if (entry == null)
            throw new IllegalArgumentException("null entry");
        return "[" + FieldType.REFERENCE.toString(entry.getObjId()) + "," + entry.index + "]";
    }

    @Override
    public int compare(ListIndexEntry entry1, ListIndexEntry entry2) {
        if (entry1 == null || entry2 == null)
            throw new IllegalArgumentException("null entry");
        int diff = FieldType.REFERENCE.compare(entry1.id, entry2.id);
        if (diff != 0)
            return diff;
        diff = Integer.compare(entry1.index, entry2.index);
        if (diff != 0)
            return diff;
        return 0;
    }

    @Override
    public ListIndexEntry fromString(ParseContext context) {
        context.expect('[');
        final ObjId id = FieldType.REFERENCE.fromString(context);
        context.expect(',');
        final int index = FieldType.INTEGER.fromString(context);
        context.expect(']');
        return new ListIndexEntry(id, index);
    }

    @Override
    protected boolean hasPrefix0xff() {
        return FieldType.REFERENCE.hasPrefix0xff();
    }

    @Override
    protected boolean hasPrefix0x00() {
        return FieldType.REFERENCE.hasPrefix0x00();
    }
}

