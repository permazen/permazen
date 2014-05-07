
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
 * Superclass for non-null types that are encoded and ordered as {@link String}s. Null values are not supported by this class.
 *
 * @param <T> The associated Java type
 */
abstract class StringEncodedType<T> extends FieldType<T> {

    private final String name;

    protected StringEncodedType(Class<T> type) {
        this(type, type.getSimpleName());
    }

    protected StringEncodedType(Class<T> type, String name) {
        super(type);
        if (name == null)
            throw new IllegalArgumentException("null name");
        this.name = name;
    }

    @Override
    public T read(ByteReader reader) {
        final String s;
        try {
            s = FieldType.STRING.read(reader);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid encoded " + this.name, e);
        }
        return this.fromString(new ParseContext(s));
    }

    @Override
    public void write(ByteWriter writer, T obj) {
        FieldType.STRING.write(writer, this.toString(obj));
    }

    @Override
    public byte[] getDefaultValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skip(ByteReader reader) {
        FieldType.STRING.skip(reader);
    }

    @Override
    public String toString(T obj) {
        if (obj == null)
            throw new IllegalArgumentException("illegal null " + this.name);
        return obj.toString();
    }

    @Override
    public int compare(T obj1, T obj2) {
        return FieldType.STRING.compare(this.toString(obj1), this.toString(obj2));
    }
}

