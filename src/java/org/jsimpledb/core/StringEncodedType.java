
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.base.Converter;

/**
 * A {@link FieldType} implementation for any Java type that can be encoded uniquely as a {@link String}.
 * A {@link Converter} is used to convert between native and {@link String} forms.
 *
 * <p>
 * This class provides a convenient way to implement custom {@link FieldType}s.
 * Null values are supported and null is the default value.
 * </p>
 *
 * @param <T> The associated Java type
 */
public class StringEncodedType<T> extends NullSafeType<T> {

    /**
     * Convenience constructor. Uses the simple name of the {@code type} as this {@link FieldType}'s type name.
     *
     * @param type represented Java type
     * @param converter converts between native form and {@link String} form
     * @throws IllegalArgumentException if any parameter is null
     */
    public StringEncodedType(Class<T> type, Converter<T, String> converter) {
        this(type, type.getSimpleName(), converter);
    }

    /**
     * Primary constructor.
     *
     * @param type represented Java type
     * @param name the name for this {@link FieldType}
     * @param converter converts between native form and {@link String} form
     * @throws IllegalArgumentException if any parameter is null
     */
    public StringEncodedType(Class<T> type, String name, Converter<T, String> converter) {
        super(new StringConvertedType<T>(type, name, converter));
    }
}

