
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.EncodingId;

/**
 * A {@link io.permazen.core.FieldType} implementation for any Java type that can be encoded uniquely as a {@link String}.
 * A {@link Converter} is used to convert between native and {@link String} forms.
 *
 * <p>
 * This class provides a convenient way to implement custom {@link io.permazen.core.FieldType}s.
 * Null values are supported and null is the default value. This type will sort instances according to
 * the lexicographical sort order of their {@link String} encodings; null will sort last.
 *
 * <p>
 * The supplied {@link Converter} must be {@link java.io.Serializable} in order for an instance of this
 * class to also be {@link java.io.Serializable}.
 *
 * @param <T> The associated Java type
 */
public class StringEncodedType<T> extends NullSafeType<T> {

    private static final long serialVersionUID = 6224434959455483181L;

    /**
     * Primary constructor.
     *
     * @param encodingId the encoding ID for this {@link io.permazen.core.FieldType}
     * @param type represented Java type
     * @param converter converts between native form and {@link String} form; should be {@link java.io.Serializable}
     * @throws IllegalArgumentException if {@code converter} does not convert null to null
     * @throws IllegalArgumentException if any parameter is null
     */
    public StringEncodedType(EncodingId encodingId, TypeToken<T> type, Converter<T, String> converter) {
        super(new StringConvertedType<T>(encodingId, type, converter));
    }

    /**
     * Convenience constructor taking {@link Class} instead of {@link TypeToken}.
     *
     * @param encodingId the encoding ID for this {@link io.permazen.core.FieldType}
     * @param type represented Java type
     * @param converter converts between native form and {@link String} form; should be {@link java.io.Serializable}
     * @throws IllegalArgumentException if {@code converter} does not convert null to null
     * @throws IllegalArgumentException if any parameter is null
     */
    public StringEncodedType(EncodingId encodingId, Class<T> type, Converter<T, String> converter) {
        super(new StringConvertedType<T>(encodingId, TypeToken.of(type), converter));
    }
}
