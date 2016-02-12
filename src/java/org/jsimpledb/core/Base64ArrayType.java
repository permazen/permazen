
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.jsimpledb.parse.ParseContext;

/**
 * For Primitive array types encode to {@link String} via Base-64 encoding of raw data. Does not support null arrays.
 *
 * @param <T> array type
 * @param <E> array element type
 */
abstract class Base64ArrayType<T, E> extends ArrayType<T, E> {

    private static final Pattern BASE64_PATTERN = Pattern.compile(
      "(([-_+/\\p{Alnum}]\\s*){4})*(([-_+/\\p{Alnum}]\\s*){2}==|([-_+/\\p{Alnum}]\\s*){3}=|=)");

    private final int size;

    Base64ArrayType(PrimitiveType<E> elementType, TypeToken<T> typeToken) {
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

    @Override
    public String toParseableString(T array) {
        String s = this.encodeString(array);
        final int length = s.length();
        if (length == 0 || s.charAt(length - 1) != '=')
            s += '=';                                                   // required in order to be self-delimiting
        return s;
    }

    @Override
    public T fromParseableString(ParseContext context) {
        if (context.peek() == '[')                                      // backward compat
            return super.fromParseableString(context);
        return this.decodeString(context.matchPrefix(BASE64_PATTERN).group());
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
        return Base64.encodeBase64String(buf.toByteArray());
    }

    private T decodeString(String base64) {
        final byte[] data = Base64.decodeBase64(base64);
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
            throw new IllegalArgumentException(this.name + " input has length " + numBytes + " not a multiple of " + this.size);
        return (T)Array.newInstance(this.elementType.typeToken.getRawType(), numBytes / this.size);
    }
}

