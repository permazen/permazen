
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.base.Converter;

import org.dellroad.stuff.string.StringEncoder;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * {@link FieldType} for any Java type that can be encoded and ordered as a {@link String}.
 * A {@link Converter} is used to convert between native and {@link String} forms.
 *
 * <p>
 * Null values are not supported by this class; instead, you would normally wrap this class in a {@link NullSafeType}.
 * </p>
 *
 * @param <T> The associated Java type
 */
class StringConvertedType<T> extends FieldType<T> {

    private final StringType stringType = new StringType();
    private final String name;
    private final Converter<T, String> converter;

    /**
     * Convenience constructor. Uses the simple name of the {@code type} as the {@link FieldType} name.
     *
     * @param type represented Java type
     * @param converter converts between native form and {@link String} form
     * @throws IllegalArgumentException if any parameter is null
     */
    protected StringConvertedType(Class<T> type, Converter<T, String> converter) {
        this(type, type.getSimpleName(), converter);
    }

    /**
     * Primary constructor.
     *
     * @param type represented Java type
     * @param name the name for this {@link FieldType}
     * @param converter converts between native form and {@link String} form
     * @throws IllegalArgumentException if any parameter is null
     */
    protected StringConvertedType(Class<T> type, String name, Converter<T, String> converter) {
        super(type);
        if (name == null)
            throw new IllegalArgumentException("null name");
        if (converter == null)
            throw new IllegalArgumentException("null converter");
        this.name = name;
        this.converter = converter;
    }

    @Override
    public T read(ByteReader reader) {
        final String string;
        try {
            string = this.stringType.read(reader);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid encoded " + this.name, e);
        }
        return this.converter.reverse().convert(string);
    }

    @Override
    public void write(ByteWriter writer, T obj) {
        this.stringType.write(writer, this.converter.convert(obj));
    }

    @Override
    public byte[] getDefaultValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skip(ByteReader reader) {
        this.stringType.skip(reader);
    }

    @Override
    public String toString(T obj) {
        if (obj == null)
            throw new IllegalArgumentException("illegal null " + this.name);
        return this.converter.convert(obj);
    }

    @Override
    public T fromString(String string) {
        try {
            return this.converter.reverse().convert(string);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("conversion from String failed", e);
        }
    }

    @Override
    public String toParseableString(T obj) {
        return StringEncoder.enquote(this.toString(obj));
    }

    @Override
    public T fromParseableString(ParseContext ctx) {
        return this.fromString(StringEncoder.dequote(ctx.matchPrefix(StringEncoder.ENQUOTE_PATTERN).group()));
    }

    @Override
    public int compare(T obj1, T obj2) {
        return this.stringType.compare(this.toString(obj1), this.toString(obj2));
    }

    @Override
    public boolean hasPrefix0x00() {
        return this.stringType.hasPrefix0x00();
    }

    @Override
    public boolean hasPrefix0xff() {
        return this.stringType.hasPrefix0xff();
    }
}

