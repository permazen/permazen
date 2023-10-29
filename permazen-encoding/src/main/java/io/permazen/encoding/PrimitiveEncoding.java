
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import io.permazen.util.ParseContext;

import org.dellroad.stuff.java.Primitive;

/**
 * Support superclass for primitive types.
 */
public abstract class PrimitiveEncoding<T> extends AbstractEncoding<T> {

    private static final long serialVersionUID = 5581526700382536487L;

    final Primitive<T> primitive;

    protected PrimitiveEncoding(Primitive<T> primitive) {
        super(EncodingIds.builtin(primitive.getName()), primitive.getType(),
          !primitive.equals(Primitive.VOID) ? primitive.getDefaultValue() : null);
        this.primitive = primitive;
    }

    @Override
    public T fromParseableString(ParseContext ctx) {
        return this.primitive.parseValue(ctx.matchPrefix(this.primitive.getParsePattern()).group());
    }

    @Override
    public String toParseableString(T value) {
        if (value == null)
            throw new IllegalArgumentException("illegal null value for primitive type " + this.primitive);
        return String.valueOf(value);
    }

    @Override
    public T validate(Object obj) {
        if (obj == null)
            throw new IllegalArgumentException("illegal null value for primitive type " + this.primitive);
        final Class<T> wrapperType = this.primitive.getWrapperType();
        try {
            return wrapperType.cast(obj);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("value " + obj + " has type "
              + obj.getClass().getName() + " but type " + wrapperType.getName() + " is required");
        }
    }

    @Override
    public int compare(T value1, T value2) {
        if (value1 == null || value2 == null)
            throw new IllegalArgumentException("illegal null value for primitive type " + this.primitive);
        return this.primitive.compare(value1, value2);
    }

// Conversion

    @Override
    public <S> T convert(Encoding<S> type, S value) {

        // Unwrap primitive wrapper types
        if (type instanceof PrimitiveWrapperEncoding) {
            if (value == null)
                throw new IllegalArgumentException("can't convert null value into primitive type " + this.primitive);
            type = ((PrimitiveWrapperEncoding<S>)type).inner;
        }

        // Handle primitive types
        if (type instanceof PrimitiveEncoding) {
            final PrimitiveEncoding<S> primitiveType = (PrimitiveEncoding<S>)type;
            if (primitiveType instanceof NumberEncoding)
                return this.convertNumber((Number)value);
            if (primitiveType instanceof BooleanEncoding)
                return this.convertNumber((Boolean)value ? 1 : 0);
            if (primitiveType instanceof CharacterEncoding)
                return this.convertNumber((int)(Character)value);
            throw new RuntimeException("internal error: " + primitiveType);
        }

        // Handle non-primitive types the regular way
        return super.convert(type, value);
    }

    protected abstract T convertNumber(Number value);

// Object

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.primitive.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final PrimitiveEncoding<?> that = (PrimitiveEncoding<?>)obj;
        return this.primitive.equals(that.primitive);
    }
}
