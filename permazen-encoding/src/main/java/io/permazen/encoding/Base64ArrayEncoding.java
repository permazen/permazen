
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Base64;
import java.util.Collections;
import java.util.regex.Pattern;

/**
 * Adds support for an alternate Base-64 {@link String} encoding for primitive array types.
 * Does not support null arrays.
 *
 * @param <T> array type
 * @param <E> array element type
 */
public abstract class Base64ArrayEncoding<T, E> extends ArrayEncoding<T, E> {

    private static final long serialVersionUID = -7770505941381986791L;

    private static final int BASE64_LINE_LENGTH = 76;
    private static final byte[] BASE64_LINE_TERMINATOR = new byte[] { (byte)'\n' };

    private static final Pattern BASE64_PATTERN = Pattern.compile(
      "((([-_+/\\p{Alnum}]\\s*){4})*)(([-_+/\\p{Alnum}]\\s*){2}==|([-_+/\\p{Alnum}]\\s*){3}=)");

    private final int size;

    protected Base64ArrayEncoding(PrimitiveEncoding<E> elementType, TypeToken<T> typeToken) {
        super(elementType, typeToken);
        this.size = elementType.primitive.getSize();
    }

// Encoding

    @Override
    public T fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        if (BASE64_PATTERN.matcher(string).matches())
            return this.decodeBase64(string);
        return super.fromString(string);
    }

    @Override
    public String toString(T array) {
        Preconditions.checkArgument(array != null, "null array");
        return this.toString(array, this.useBase64Encoding(array));
    }

// Other Methods

    /**
     * Encode a non-null value as a {@link String} for later decoding by {@link #fromString fromString()}.
     *
     * @param array array to encode
     * @param base64 true for base 64 synax, false for standard syntax
     * @return string encoding of {@code value} acceptable to {@link #fromString fromString()}
     * @throws IllegalArgumentException if {@code value} is null
     */
    public String toString(T array, boolean base64) {
        return base64 ? this.encodeBase64(array) : super.toString(array);
    }

    /**
     * Invoked by {@link #toString(T)} to determine whether to encode using base 64 or not.
     *
     * <p>
     * The implementation in {@link Base64ArrayEncoding} rerturns true for arrays longer than 16 elements.
     *
     * @param array array to be encoded, never null
     * @return true for base 64 encoding, otherwise false
     */
    protected boolean useBase64Encoding(T array) {
        return this.getArrayLength(array) > 16;
    }

    private String encodeBase64(T array) {
        Preconditions.checkArgument(array != null, "null array");
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

    private T decodeBase64(String base64) {
        Preconditions.checkArgument(base64 != null, "null base64");
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
        if (numBytes % this.size != 0) {
            throw new IllegalArgumentException(String.format(
              "input has length %d which is not a multiple of %d", numBytes, this.size));
        }
        return (T)Array.newInstance(this.elementType.getTypeToken().getRawType(), numBytes / this.size);
    }
}
