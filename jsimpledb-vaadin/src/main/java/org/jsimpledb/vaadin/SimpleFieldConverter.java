
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.vaadin;

import com.google.common.base.Preconditions;
import com.vaadin.data.util.converter.Converter;

import java.util.Locale;

import org.jsimpledb.core.FieldType;

/**
 * Vaadin {@link Converter} for {@link FieldType}s to/from {@link String}.
 * Trims whitespace before converting from {@link String}.
 *
 * @param <T> field type
 */
@SuppressWarnings("serial")
public class SimpleFieldConverter<T> implements Converter<String, T> {

    private final FieldType<T> fieldType;

    public SimpleFieldConverter(FieldType<T> fieldType) {
        Preconditions.checkArgument(fieldType != null, "null fieldType");
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
        if (value == null)
            return null;
        return this.fieldType.toString(value);
    }

    @Override
    public T convertToModel(String value, Class<? extends T> targetType, Locale locale) {
        if (value == null)
            return null;
        try {
            return this.fieldType.fromString(value.trim());
        } catch (IllegalArgumentException e) {
            throw new Converter.ConversionException("invalid value of type `" + this.fieldType + "': " + e.getMessage());
        }
    }
}

