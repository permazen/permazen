
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.encoding.Encoding;
import io.permazen.schema.AbstractEnumSchemaField;

import java.lang.reflect.Method;

import org.dellroad.stuff.java.EnumUtil;

/**
 * Support superclass for {@link PermazenSimpleField}'s that involve an {@link Enum} type.
 *
 * @param <A> the field's Java type
 * @param <B> the field's core API encoding type
 */
abstract class AbstractEnumPermazenField<A, B> extends ConvertedSimplePermazenField<A, B> {

    final Class<? extends Enum<?>> enumType;

    AbstractEnumPermazenField(String name, int storageId, TypeToken<A> typeToken, Encoding<B> encoding,
      Class<? extends Enum<?>> enumType, boolean indexed, io.permazen.annotation.PermazenField annotation,
      String description, Method getter, Method setter, Converter<A, B> converter) {
        super(name, storageId, typeToken, encoding, indexed, annotation, description, getter, setter, converter);
        Preconditions.checkArgument(enumType != null, "null enumType");
        this.enumType = enumType;
    }

// Public Methods

    /**
     * Get the {@link Enum} type that this field uses.
     *
     * @return this field's {@link Enum} type
     */
    public Class<? extends Enum<?>> getEnumType() {
        return this.enumType;
    }

// Package Methods

    @Override
    AbstractEnumSchemaField toSchemaItem() {
        final AbstractEnumSchemaField schemaField = (AbstractEnumSchemaField)super.toSchemaItem();
        EnumUtil.getValues(this.enumType).forEach(value -> schemaField.getIdentifiers().add(value.name()));
        return schemaField;
    }

    @Override
    abstract AbstractEnumSchemaField createSchemaItem();
}
