
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.encoding.Encoding;
import io.permazen.schema.AbstractEnumSchemaField;
import io.permazen.schema.SimpleSchemaField;

import java.lang.reflect.Method;

import org.dellroad.stuff.java.EnumUtil;

/**
 * Support superclass for {@link JSimpleField}'s that involve an {@link Enum} type.
 */
abstract class AbstractEnumJSimpleField<A, B> extends ConvertedJSimpleField<A, B> {

    final Class<? extends Enum<?>> enumType;

    AbstractEnumJSimpleField(Permazen jdb, String name, int storageId, TypeToken<A> typeToken, Encoding<B> encoding,
      Class<? extends Enum<?>> enumType, boolean indexed, io.permazen.annotation.JField annotation, String description,
      Method getter, Method setter, Converter<A, B> converter) {
        super(jdb, name, storageId, typeToken, encoding, indexed, annotation, description, getter, setter, converter);
        Preconditions.checkArgument(enumType != null, "null enumType");
        this.enumType = enumType;
    }

    @Override
    @SuppressWarnings("unchecked")
    void initialize(Permazen jdb, SimpleSchemaField schemaField0) {
        super.initialize(jdb, schemaField0);
        final AbstractEnumSchemaField schemaField = (AbstractEnumSchemaField)schemaField0;
        schemaField.getIdentifiers().clear();
        for (Enum<?> value : EnumUtil.getValues(this.enumType))
            schemaField.getIdentifiers().add(value.name());
    }

    @Override
    void initializeSimpleSchemaFieldEncoding(SimpleSchemaField schemaField) {
        // fixed encoding
    }

    @Override
    @SuppressWarnings("unchecked")
    Class<? extends Enum<?>> getEnumType() {
        return this.enumType;
    }
}
