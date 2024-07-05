
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ParseContext;

import java.util.ArrayList;
import java.util.List;

import org.dellroad.stuff.string.StringEncoder;

/**
 * Support superclass for non-null built-in array {@link Encoding}s.
 *
 * <p>
 * The string form looks like {@code [ "elem1", "elem2", ..., "elemN" ]}.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Arrays sort lexicographically.
 *
 * @param <T> array type
 * @param <E> array element type
 */
public abstract class ArrayEncoding<T, E> extends AbstractEncoding<T> {

    private static final long serialVersionUID = 3776218636387986632L;

    final Encoding<E> elementEncoding;

    /**
     * Constructor.
     *
     * @param elementEncoding array element type (possibly also an {@link ArrayEncoding})
     * @param typeToken array type token
     * @throws IllegalArgumentException if {@code elementEncoding} is an {@link ArrayEncoding} with
     *  {@link Encoding#MAX_ARRAY_DIMENSIONS} dimensions
     */
    @SuppressWarnings("unchecked")
    protected ArrayEncoding(Encoding<E> elementEncoding, TypeToken<T> typeToken) {
        super(typeToken);
        this.elementEncoding = elementEncoding;
    }

    /**
     * Get the element type.
     *
     * @return element type
     */
    public Encoding<E> getElementEncoding() {
        return this.elementEncoding;
    }

    @Override
    public String toString(T array) {
        Preconditions.checkArgument(array != null, "null array");
        final int length = this.getArrayLength(array);
        final String[] elements = new String[length];
        for (int i = 0; i < length; i++) {
            final E element = this.getArrayElement(array, i);
            if (element != null)
                elements[i] = this.elementEncoding.toString(element);
        }
        return ArrayEncoding.toArrayString(elements, true);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T fromString(String string) {
        final String[] elements = ArrayEncoding.fromArrayString(string);
        final ArrayList<E> list = new ArrayList<>(elements.length);
        for (String element : elements)
            list.add(element != null ? this.elementEncoding.fromString(element) : null);
        return this.createArray(list);
    }

    @Override
    public int compare(T array1, T array2) {
        final int length1 = this.getArrayLength(array1);
        final int length2 = this.getArrayLength(array2);
        int i = 0;
        while (i < length1 && i < length2) {
            int diff = this.elementEncoding.compare(this.getArrayElement(array1, i), this.getArrayElement(array2, i));
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
        return super.hashCode() ^ this.elementEncoding.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ArrayEncoding<?, ?> that = (ArrayEncoding<?, ?>)obj;
        return this.elementEncoding.equals(that.elementEncoding);
    }

    @Override
    public boolean supportsNull() {
        return false;
    }

    // Array classes do not implement Comparable
    @Override
    public boolean sortsNaturally() {
        return false;
    }

// Conversion

    @Override
    public <S> T convert(Encoding<S> type, S value) {

        // Handle null
        if (value == null)
            throw new IllegalArgumentException("invalid null value");

        // Unwrap nullable types
        if (type instanceof NullSafeEncoding)
            type = ((NullSafeEncoding<S>)type).inner;

        // For array types, try to convert element-by-element
        if (type instanceof ArrayEncoding)
            return this.convertArray((ArrayEncoding<S, ?>)type, value);

        // Defer to superclass
        return super.convert(type, value);
    }

    private <S, F> T convertArray(ArrayEncoding<S, F> that, S value) {
        final int length = that.getArrayLength(value);
        final ArrayList<E> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++)
            list.add(this.elementEncoding.convert(that.elementEncoding, that.getArrayElement(value, i)));
        return this.createArray(list);
    }

// Stringification

    /**
     * Create a single, combined {@link String} representation of an array of {@link String}s.
     *
     * @param strings the individual strings
     * @param spacing true to include spaces around each element
     * @return combined array string
     * @throws IllegalArgumentException if {@code strings} is null
     */
    public static String toArrayString(String[] strings, boolean spacing) {
        Preconditions.checkArgument(strings != null, "null strings");
        final StringBuilder buf = new StringBuilder();
        buf.append('[');
        for (String string : strings) {
            if (buf.length() > 1)
                buf.append(',');
            if (spacing)
                buf.append(' ');
            buf.append(string != null ? StringEncoder.enquote(string) : "null");
        }
        if (spacing && buf.length() > 1)
            buf.append(' ');
        buf.append(']');
        return buf.toString();
    }

    /**
     * Parse a combined {@link String} representation of an array of {@link String}s and return the individual strings.
     *
     * <p>
     * This method inverts {@link ArrayEncoding#toArrayString ArrayEncoding.toArrayString()}. Extra whitespace is ignored.
     *
     * @param string the array string
     * @return array of original strings
     * @throws IllegalArgumentException if {@code string} is null
     */
    public static String[] fromArrayString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        final ParseContext ctx = new ParseContext(string);
        final ArrayList<String> list = new ArrayList<>();
        ctx.skipWhitespace();
        ctx.expect('[');
        while (true) {
            ctx.skipWhitespace();
            if (ctx.tryLiteral("]")) {
                ctx.skipWhitespace();
                break;
            }
            if (!list.isEmpty()) {
                ctx.expect(',');
                ctx.skipWhitespace();
            }
            if (ctx.tryPattern("null\\b") != null)
                list.add(null);
            else {
                final String quote = ctx.matchPrefix(StringEncoder.ENQUOTE_PATTERN).group();
                list.add(StringEncoder.dequote(quote));
            }
        }
        Preconditions.checkArgument(ctx.isEOF(), "trailing garbage after array");
        return list.toArray(new String[list.size()]);
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
