
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.util.ParseContext;

/**
 * Support superclass for primitive types.
 */
abstract class PrimitiveType<T> extends NonNullFieldType<T> {

    private static final long serialVersionUID = 5581526700382536487L;

    final Primitive<T> primitive;

    PrimitiveType(Primitive<T> primitive) {
        super(primitive.getName(), TypeToken.of(primitive.getType()), 0,
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
        final PrimitiveType<?> that = (PrimitiveType<?>)obj;
        return this.primitive.equals(that.primitive);
    }
}

