
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.ParseContext;

import org.dellroad.stuff.string.StringEncoder;

/**
 * {@link io.permazen.core.FieldType} for any Java type that can be encoded and ordered as a {@link String}.
 * A {@link Converter} is used to convert between native and {@link String} forms.
 *
 * <p>
 * Null values are not supported by this class; instead, use {@link StringEncodedType}, which is the
 * null-supporting wrapper around this class.
 *
 * <p>
 * The given {@link Converter} must be {@link java.io.Serializable} in order for an instance of this
 * class to also be {@link java.io.Serializable}.
 *
 * @param <T> The associated Java type
 */
public class StringConvertedType<T> extends NonNullFieldType<T> {

    private static final long serialVersionUID = -2432755812735736593L;

    private final StringType stringType = new StringType();
    private final Converter<T, String> converter;

    /**
     * Primary constructor.
     *
     * @param name the name for this {@link io.permazen.core.FieldType}
     * @param type represented Java type
     * @param signature binary encoding signature (in this case, {@link String} encoding signature)
     * @param converter converts between native form and {@link String} form; should be {@link java.io.Serializable}
     * @throws IllegalArgumentException if any parameter is null
     */
    protected StringConvertedType(String name, TypeToken<T> type, long signature, Converter<T, String> converter) {
        super(name, type, signature);
        Preconditions.checkArgument(converter != null, "null converter");
        Preconditions.checkArgument(converter.convert(null) == null && converter.reverse().convert(null) == null,
          "invalid converter: does not convert null <-> null");
        this.converter = converter;
    }

    @Override
    public T read(ByteReader reader) {
        final String string;
        try {
            string = this.stringType.read(reader);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid encoded " + this.getName(), e);
        }
        return this.converter.reverse().convert(string);
    }

    @Override
    public void write(ByteWriter writer, T obj) {
        this.stringType.write(writer, this.converter.convert(obj));
    }

    @Override
    public void skip(ByteReader reader) {
        this.stringType.skip(reader);
    }

    @Override
    public String toString(T obj) {
        if (obj == null)
            throw new IllegalArgumentException("illegal null " + this.getName());
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

