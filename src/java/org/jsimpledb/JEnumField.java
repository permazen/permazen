
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;

import org.dellroad.stuff.java.EnumUtil;
import org.jsimpledb.schema.EnumSchemaField;
import org.jsimpledb.schema.SimpleSchemaField;

/**
 * Represents an enum field in a {@link JClass}.
 */
public class JEnumField extends JSimpleField {

    private final EnumConverter<?> converter;

    JEnumField(String name, int storageId, Class<? extends Enum<?>> enumType,
      boolean indexed, String description, Method getter, Method setter) {
        super(name, storageId, TypeToken.of(enumType.asSubclass(Enum.class)),
          enumType.getName(), indexed, description, getter, setter);
        this.converter = EnumConverter.createEnumConverter(enumType);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJEnumField(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeToken<? extends Enum<?>> getType() {
        return (TypeToken<? extends Enum<?>>)this.typeToken;
    }

    @Override
    public Converter<?, ?> getConverter(JTransaction jtx) {
        return this.converter.reverse();
    }

    @Override
    EnumSchemaField toSchemaItem() {
        final EnumSchemaField schemaField = new EnumSchemaField();
        this.initialize(schemaField);
        return schemaField;
    }

    @SuppressWarnings("unchecked")
    void initialize(SimpleSchemaField schemaField0) {
        super.initialize(schemaField0);
        final EnumSchemaField schemaField = (EnumSchemaField)schemaField0;
        schemaField.getIdentifiers().clear();
        for (Enum<?> value : (Iterable<Enum<?>>)EnumUtil.getValues((Class<Enum>)this.getType().getRawType()))
            schemaField.getIdentifiers().add(value.name());
    }
}

