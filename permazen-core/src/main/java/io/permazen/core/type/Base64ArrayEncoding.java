
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.reflect.TypeToken;

import io.permazen.util.ParseContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Base64;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * For Primitive array types encode to {@link String} via Base-64 encoding of raw data. Does not support null arrays.
 *
 * <p>
 * Note: in order to return a {@link String} that is self-delimiting, {@link #toParseableString toParseableString()} appends
 * an extra {@code "="} character when the length of the array is equal to 3 (mod 4).
 *
 * @param <T> array type
 * @param <E> array element type
 */
public abstract class Base64ArrayEncoding<T, E> extends ArrayEncoding<T, E> {

    private static final long serialVersionUID = -7770505941381986791L;

    private static final int BASE64_LINE_LENGTH = 76;
    private static final byte[] BASE64_LINE_TERMINATOR = new byte[] { (byte)'\n' };

    private static final Pattern BASE64_PATTERN = Pattern.compile(
      "((([-_+/\\p{Alnum}]\\s*){4})*)(([-_+/\\p{Alnum}]\\s*){2}==|([-_+/\\p{Alnum}]\\s*){3}=|=)");

    private final int size;

    protected Base64ArrayEncoding(PrimitiveEncoding<E> elementType, TypeToken<T> typeToken) {
        super(elementType, typeToken);
        this.size = elementType.primitive.getSize();
    }

    @Override
    public T fromString(String string) {
        if (string.length() == 0)
            return this.createArray(Collections.<E>emptyList());
        if (string.charAt(0) == '[')                                    // backward compat
            return super.fromString(string);
        return this.decodeString(string);
    }

    @Override
    public String toString(T array) {
        return this.encodeString(array).toString();
    }

    /**
     * Encode a non-null value as a {@link String} for later decoding by {@link #fromString fromString()}.
     *
     * <p>
     * This class supports two {@link String} encodings: base 64 and "list" syntax with square brackets and commas.
     * The method {@link #toString(Object)} returns the base 64 form; this method works exactly the same way but
     * allows the caller to specify which form to generate. Either form is parseable by {@link #fromString fromString()}.
     *
     * @param array array to encode, never null
     * @param base64 true for base 64 synax, false for list syntax
     * @return string encoding of {@code value} acceptable to {@link #fromString fromString()}
     * @throws IllegalArgumentException if {@code value} is null
     */
    public String toString(T array, boolean base64) {
        return base64 ? this.toString(array) : super.toString(array);
    }

    @Override
    public String toParseableString(T array) {
        String s = this.encodeString(array);
        final int length = s.length();
        if (length == 0 || s.charAt(length - 1) != '=')
            s += '=';                                                   // required in order to be self-delimiting
        return s;
    }

    /**
     * Encode a possibly null value as a {@link String} for later decoding by {@link #fromParseableString fromParseableString()}.
     *
     * <p>
     * This class supports two {@link String} encodings: base 64 and "list" syntax with square brackets and commas.
     * The method {@link #toParseableString(Object)} returns the base 64 form; this method works exactly the same way but
     * allows the caller to specify which form to generate. Either form is parseable by
     * {@link #fromParseableString fromParseableString()}.
     *
     * @param array array to encode, possibly null
     * @param base64 true for base 64 synax, false for list syntax
     * @return string encoding of {@code value} acceptable to {@link #fromParseableString fromParseableString()}
     * @throws IllegalArgumentException if {@code value} is null and this type does not support null
     */
    public String toParseableString(T array, boolean base64) {
        return base64 ? this.toParseableString(array) : super.toParseableString(array);
    }

    @Override
    public T fromParseableString(ParseContext context) {

        // Check for "list" syntax
        if (context.peek() == '[')
            return super.fromParseableString(context);

        // Strip off extra trailing "=", if any
        final Matcher matcher = context.matchPrefix(BASE64_PATTERN);
        final String head = matcher.group(1);
        final String tail = matcher.group(4);
        String base64 = head;
        if (!tail.equals("="))
            base64 += tail;

        // Decode base 64
        return this.decodeString(base64);
    }

    private String encodeString(T array) {
        final int length = this.getArrayLength(array);
        if (length == 0)
            return "";
        final ByteArrayOutputStream buf = new ByteArrayOutputStream(length * this.size);
        try (DataOutputStream output = new DataOutputStream(buf)) {
            this.encode(array, output);
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }
        return Base64.getMimeEncoder(BASE64_LINE_LENGTH, BASE64_LINE_TERMINATOR).encodeToString(buf.toByteArray());
    }

    private T decodeString(String base64) {
        if (base64.trim().isEmpty())
            return this.createArray(Collections.<E>emptyList());
        base64 = base64.replace('-', '+').replace('_', '/');            // undo URL-safe mode, if needed
        final byte[] data = Base64.getMimeDecoder().decode(base64);
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(data))) {
            return this.decode(input, data.length);
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }
    }

    protected abstract void encode(T array, DataOutputStream output) throws IOException;

    protected abstract T decode(DataInputStream input, int numBytes) throws IOException;

    @SuppressWarnings("unchecked")
    protected T checkDecodeLength(int numBytes) {
        if (numBytes % this.size != 0)
            throw new IllegalArgumentException("input has length " + numBytes + " which is not a multiple of " + this.size);
        return (T)Array.newInstance(this.elementType.getTypeToken().getRawType(), numBytes / this.size);
    }
}
