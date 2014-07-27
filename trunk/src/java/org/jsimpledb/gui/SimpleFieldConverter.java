
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.data.util.converter.Converter;

import java.util.Locale;

import org.jsimpledb.core.FieldType;
import org.jsimpledb.parse.ParseContext;

/**
 * Vaadin {@link Converter} for {@link FieldType}s to/from {@link String}.
 *
 * @param <T> field type
 */
@SuppressWarnings("serial")
public class SimpleFieldConverter<T> implements Converter<String, T> {

    private final FieldType<T> fieldType;

    public SimpleFieldConverter(FieldType<T> fieldType) {
        if (fieldType == null)
            throw new IllegalArgumentException("null fieldType");
        this.fieldType = fieldType;
    }

    public Class<String> getPresentationType() {
        return String.class;
    }

    @SuppressWarnings("unchecked")
    public Class<T> getModelType() {
        return (Class<T>)this.fieldType.getTypeToken().getRawType();
    }

    @Override
    public String convertToPresentation(T value, Class<? extends String> targetType, Locale locale) {
        return this.fieldType.toString(value);
    }

    @Override
    public T convertToModel(String value, Class<? extends T> targetType, Locale locale) {
        final T result;
        final ParseContext ctx = new ParseContext(value.trim());
        try {
            result = this.fieldType.fromString(ctx);
        } catch (IllegalArgumentException e) {
            throw new Converter.ConversionException("invalid value of type `" + this.fieldType + "': " + e.getMessage());
        }
        if (!ctx.isEOF())
            throw new Converter.ConversionException("extra trailing garbage in value of type `" + this.fieldType + "'");
        return result;
    }
}

