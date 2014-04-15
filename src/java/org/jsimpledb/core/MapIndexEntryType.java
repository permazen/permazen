
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
 * Support superclass for {@link FieldType}s of {@link MapIndexEntry} subclassess.
 *
 * @param <E> map index entry type
 * @param <T> map sub-field value type
 */
abstract class MapIndexEntryType<E extends MapIndexEntry<T>, T> extends FieldType<E> {

    private final FieldType<T> otherType;

    @SuppressWarnings("serial")
    MapIndexEntryType(String name, TypeToken<E> typeToken, FieldType<T> otherType) {
        super(name, typeToken);
        this.otherType = otherType;
    }

    @Override
    public E read(ByteReader reader) {
        final ObjId id = FieldType.REFERENCE.read(reader);
        final T other = this.otherType.read(reader);
        return this.createMapIndexEntry(id, other);
    }

    @Override
    public void copy(ByteReader reader, ByteWriter writer) {
        FieldType.REFERENCE.copy(reader, writer);
        this.otherType.copy(reader, writer);
    }

    @Override
    public void write(ByteWriter writer, E entry) {
        if (entry == null)
            throw new IllegalArgumentException("null entry");
        FieldType.REFERENCE.write(writer, entry.getObjId());
        this.otherType.write(writer, entry.other);
    }

    @Override
    public byte[] getDefaultValue() {
        final byte[] b1 = FieldType.REFERENCE.getDefaultValue();
        final byte[] b2 = this.otherType.getDefaultValue();
        final byte[] result = new byte[b1.length + b2.length];
        System.arraycopy(b1, 0, result, 0, b1.length);
        System.arraycopy(b2, 0, result, b1.length, b2.length);
        return result;
    }

    @Override
    public void skip(ByteReader reader) {
        FieldType.REFERENCE.skip(reader);
        this.otherType.skip(reader);
    }

    @Override
    public E validate(Object obj) {
        if (obj == null)
            throw new IllegalArgumentException("illegal null value");
        return super.validate(obj);
    }

    @Override
    public boolean hasPrefix0xff() {
        return FieldType.REFERENCE.hasPrefix0xff();
    }

    @Override
    public boolean hasPrefix0x00() {
        return FieldType.REFERENCE.hasPrefix0x00();
    }

    @Override
    public String toString(E entry) {
        if (entry == null)
            throw new IllegalArgumentException("null entry");
        return "[" + FieldType.REFERENCE.toString(entry.id) + "," + this.otherType.toString(entry.other) + "]";
    }

    @Override
    public E fromString(ParseContext ctx) {
        ctx.expect('[');
        final ObjId id = FieldType.REFERENCE.fromString(ctx);
        ctx.expect(',');
        final T other = this.otherType.fromString(ctx);
        ctx.expect(']');
        return this.createMapIndexEntry(id, other);
    }

    protected abstract E createMapIndexEntry(ObjId id, T other);

    @Override
    public int compare(E entry1, E entry2) {
        if (entry1 == null || entry2 == null)
            throw new IllegalArgumentException("null entry");
        int diff = FieldType.REFERENCE.compare(entry1.id, entry2.id);
        if (diff != 0)
            return diff;
        diff = this.otherType.compare(entry1.other, entry2.other);
        if (diff != 0)
            return diff;
        return 0;
    }

// Object

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.otherType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final MapIndexEntryType<?, ?> that = (MapIndexEntryType<?, ?>)obj;
        return this.otherType.equals(that.otherType);
    }
}

