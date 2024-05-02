
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeToken;

/**
 * {@link Encoding} for any Java type that can be encoded and ordered as a {@link String}.
 * A Guava {@link Converter} is used to convert between native and {@link String} forms.
 *
 * <p>
 * This class provides a convenient way to implement custom {@link Encoding}s.
 * Null values are supported and null is the default value. This type will sort instances according to
 * the lexicographical sort order of their {@link String} encodings; null will sort last.
 *
 * @param <T> The associated Java type
 * @see ConvertedEncoding
 */
public class StringConvertedEncoding<T> extends ConvertedEncoding<T, String> {

    private static final long serialVersionUID = -1937834783878909370L;

    /**
     * Primary constructor.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param type represented Java type
     * @param converter value converter
     * @throws IllegalArgumentException if any parameter is null
     */
    public StringConvertedEncoding(EncodingId encodingId, TypeToken<T> type, Converter<T, String> converter) {
        super(encodingId, type, null, new NullSafeEncoding<>(null, new StringEncoding(null)), converter, false);
    }

    /**
     * Convenience constructor taking {@link Class} instead of {@link TypeToken}.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param type represented Java type
     * @param converter converts between native form and {@link String} form; should be {@link java.io.Serializable}
     * @throws IllegalArgumentException if any parameter is null
     */
    public StringConvertedEncoding(EncodingId encodingId, Class<T> type, Converter<T, String> converter) {
        this(encodingId, TypeToken.of(type), converter);
    }
}
