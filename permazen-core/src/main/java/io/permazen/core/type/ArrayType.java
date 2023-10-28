
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.reflect.TypeToken;

import io.permazen.core.AbstractEncoding;
import io.permazen.core.Encoding;
import io.permazen.core.EncodingId;
import io.permazen.util.ParseContext;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Support superclass for builtin array {@link Encoding}s.
 *
 * <p>
 * The string form looks like {@code [ elem1, elem2, ..., elemN ]}.
 *
 * <p>
 * This class does not support null arrays; wrap in {@link NullSafeType} to get that.
 * The default value is the empty array.
 *
 * <p>
 * Arrays sort lexicographically.
 *
 * @param <T> array type
 * @param <E> array element type
 */
public abstract class ArrayType<T, E> extends AbstractEncoding<T> {

    private static final long serialVersionUID = 3776218636387986632L;

    final Encoding<E> elementType;

    /**
     * Constructor.
     *
     * @param elementType array element type (possibly also an {@link ArrayType})
     * @param typeToken array type token
     * @throws IllegalArgumentException if {@code elementType} is an {@link ArrayType} with
     *  {@link Encoding#MAX_ARRAY_DIMENSIONS} dimensions
     */
    @SuppressWarnings("unchecked")
    protected ArrayType(Encoding<E> elementType, TypeToken<T> typeToken) {
        super(Optional.ofNullable(elementType.getEncodingId()).map(EncodingId::getArrayId).orElse(null),
          typeToken, (T)Array.newInstance(elementType.getTypeToken().getRawType(), 0));
        this.elementType = elementType;
    }

    /**
     * Get the element type.
     *
     * @return element type
     */
    public Encoding<E> getElementType() {
        return this.elementType;
    }

    @Override
    public String toParseableString(T array) {
        assert array != null;
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
        return super.hashCode() ^ this.elementType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ArrayType<?, ?> that = (ArrayType<?, ?>)obj;
        return this.elementType.equals(that.elementType);
    }

// Conversion

    @Override
    public <S> T convert(Encoding<S> type, S value) {

        // Handle null
        if (value == null)
            throw new IllegalArgumentException("invalid null value");

        // Unwrap nullable types
        if (type instanceof NullSafeType)
            type = ((NullSafeType<S>)type).inner;

        // For array types, try to convert element-by-element
        if (type instanceof ArrayType)
            return this.convertArray((ArrayType<S, ?>)type, value);

        // Defer to superclass
        return super.convert(type, value);
    }

    private <S, F> T convertArray(ArrayType<S, F> that, S value) {
        final int length = that.getArrayLength(value);
        final ArrayList<E> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++)
            list.add(this.elementType.convert(that.elementType, that.getArrayElement(value, i)));
        return this.createArray(list);
    }

// Subclass overrides

    /**
     * Get the length of the given array.
     *
     * @param array non-null array
     * @return array length
     */
    protected abstract int getArrayLength(T array);

    /**
     * Get an element from the given array.
     *
     * @param array non-null array
     * @param index index of target element in {@code array}
     * @return array element at index {@code index}
     */
    protected abstract E getArrayElement(T array, int index);

    /**
     * Create a new array instance containing the given elements.
     *
     * @param elements content for the new array
     * @return newly created array
     */
    protected abstract T createArray(List<E> elements);
}
