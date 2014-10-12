
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.List;

import org.jsimpledb.parse.ParseContext;

/**
 * Superclass for all array {@link FieldType}s.
 *
 * <p>
 * This class does not support null arrays; wrap in {@link NullSafeType} to get that.
 * </p>
 *
 * <p>
 * Arrays sort lexicographically.
 * </p>
 *
 * @param <T> array type
 * @param <E> array element type
 */
abstract class ArrayType<T, E> extends NonNullFieldType<T> {

    /**
     * Array type name suffix.
     */
    public static final String ARRAY_SUFFIX = "[]";

    /**
     * Maximum allowed array dimensions ({@value #MAX_DIMENSIONS}).
     */
    public static final int MAX_DIMENSIONS = 255;

    final FieldType<E> elementType;
    final int dimensions;

    /**
     * Constructor.
     *
     * @param elementType array element type (possibly also an {@link ArrayType})
     * @param typeToken array type token
     */
    protected ArrayType(FieldType<E> elementType, TypeToken<T> typeToken) {
        super(elementType.name + "[]", typeToken);
        this.elementType = elementType;
        this.dimensions = elementType instanceof ArrayType ? ((ArrayType)elementType).dimensions + 1 : 1;
        if (this.dimensions > MAX_DIMENSIONS)
            throw new IllegalArgumentException("too many array dimensions");
    }

    /**
     * Get the number of array dimensions.
     *
     * @return array dimensions, a value between 1 and {@link #MAX_DIMENSIONS} (inclusive)
     */
    public int getDimensions() {
        return this.dimensions;
    }

    @Override
    public String toParseableString(T array) {
        if (array == null)
            return "null";
        final StringBuilder buf = new StringBuilder();
        buf.append('[');
        final int length = this.getArrayLength(array);
        for (int i = 0; i < length; i++) {
            if (i > 0)
                buf.append(',').append(' ');
            buf.append(this.elementType.toParseableString(this.getArrayElement(array, i)));
        }
        buf.append(']');
        return buf.toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T fromParseableString(ParseContext context) {
        if (context.tryLiteral("null"))
            return null;
        final ArrayList<E> list = new ArrayList<>();
        context.expect('[');
        while (true) {
            context.skipWhitespace();
            if (context.tryLiteral("]"))
                break;
            if (!list.isEmpty()) {
                context.expect(',');
                context.skipWhitespace();
            }
            list.add(this.elementType.fromParseableString(context));
        }
        return this.createArray(list);
    }

    @Override
    public int compare(T array1, T array2) {
        final int length1 = this.getArrayLength(array1);
        final int length2 = this.getArrayLength(array2);
        int i = 0;
        while (i < length1 && i < length2) {
            int diff = this.elementType.compare(this.getArrayElement(array1, i), this.getArrayElement(array2, i));
            if (diff != 0)
                return diff;
            i++;
        }
        if (i < length2)
            return -1;
        if (i < length1)
            return 1;
        return 0;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.elementType.hashCode() ^ this.dimensions;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ArrayType<?, ?> that = (ArrayType<?, ?>)obj;
        return this.elementType.equals(that.elementType) && this.dimensions == that.dimensions;
    }

// Subclass overrides

    /**
     * Get the length of the given array.
     *
     * @param array non-null array
     */
    protected abstract int getArrayLength(T array);

    /**
     * Get an element from the given array.
     *
     * @param array non-null array
     * @param index index of target element in {@code array}
     */
    protected abstract E getArrayElement(T array, int index);

    /**
     * Create a new array instance containing the given elements.
     *
     * @param elements content for the new array
     */
    protected abstract T createArray(List<E> elements);
}

