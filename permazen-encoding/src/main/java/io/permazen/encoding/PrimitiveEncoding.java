
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import org.dellroad.stuff.java.Primitive;

/**
 * Support superclass for built-in primitive type encodings.
 */
public abstract class PrimitiveEncoding<T> extends AbstractEncoding<T> {

    private static final long serialVersionUID = 5581526700382536487L;

    final Primitive<T> primitive;

    protected PrimitiveEncoding(EncodingId encodingId, Primitive<T> primitive) {
        super(encodingId, primitive.getType(), primitive::getDefaultValue);
        this.primitive = primitive;
    }

    // For VoidEncoding only
    protected PrimitiveEncoding(Primitive<T> primitive) {
        super(primitive.getType());
        this.primitive = primitive;
    }

    @Override
    public T fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return this.primitive.parseValue(string);
    }

    @Override
    public String toString(T value) {
        if (value == null)
            throw new IllegalArgumentException(String.format("illegal null value for primitive type %s", this.primitive));
        return String.valueOf(value);
    }

    @Override
    public T validate(Object obj) {
        if (obj == null)
            throw new IllegalArgumentException(String.format("illegal null value for primitive type %s", this.primitive));
        final Class<T> wrapperType = this.primitive.getWrapperType();
        try {
            return wrapperType.cast(obj);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(String.format(
              "value %s has type %s but type %s is required", obj, obj.getClass().getName(), wrapperType.getName()));
        }
    }

    @Override
    public int compare(T value1, T value2) {
        if (value1 == null || value2 == null)
            throw new IllegalArgumentException(String.format("illegal null value for primitive type %s", this.primitive));
        return this.primitive.compare(value1, value2);
    }

    @Override
    public boolean supportsNull() {
        return false;
    }

    @Override
    public boolean sortsNaturally() {
        return true;
    }

// Conversion

    @Override
    public <S> T convert(Encoding<S> type, S value) {

        // Unwrap primitive wrapper types
        if (type instanceof PrimitiveWrapperEncoding) {
            if (value == null) {
                throw new IllegalArgumentException(String.format(
                  "can't convert null value into primitive type %s", this.primitive));
            }
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
            throw new RuntimeException(String.format("internal error: %s", primitiveType));
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
