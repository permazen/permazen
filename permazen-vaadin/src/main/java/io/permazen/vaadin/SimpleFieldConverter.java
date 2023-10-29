
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.google.common.base.Preconditions;
import com.vaadin.data.util.converter.Converter;

import io.permazen.core.encoding.Encoding;

import java.util.Locale;

/**
 * Vaadin {@link Converter} for {@link Encoding}s to/from {@link String}.
 * Trims whitespace before converting from {@link String}.
 *
 * @param <T> encoding
 */
@SuppressWarnings("serial")
public class SimpleFieldConverter<T> implements Converter<String, T> {

    private final Encoding<T> encoding;

    public SimpleFieldConverter(Encoding<T> encoding) {
        Preconditions.checkArgument(encoding != null, "null encoding");
        this.encoding = encoding;
    }

    @Override
    public Class<String> getPresentationType() {
        return String.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<T> getModelType() {
        return (Class<T>)this.encoding.getTypeToken().getRawType();
    }

    @Override
    public String convertToPresentation(T value, Class<? extends String> targetType, Locale locale) {
        if (value == null)
            return null;
        return this.encoding.toString(value);
    }

    @Override
    public T convertToModel(String value, Class<? extends T> targetType, Locale locale) {
        if (value == null)
            return null;
        try {
            return this.encoding.fromString(value.trim());
        } catch (IllegalArgumentException e) {
            throw new Converter.ConversionException("invalid value of type \"" + this.encoding + "\": " + e.getMessage());
        }
    }
}
