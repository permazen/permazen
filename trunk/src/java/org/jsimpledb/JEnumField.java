
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
import org.jsimpledb.core.EnumFieldType;
import org.jsimpledb.core.EnumValue;
import org.jsimpledb.schema.EnumSchemaField;
import org.jsimpledb.schema.SimpleSchemaField;

/**
 * Represents an {@link Enum} field in a {@link JClass}.
 */
public class JEnumField extends JSimpleField {

    final EnumConverter<?> converter;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    JEnumField(JSimpleDB jdb, String name, int storageId, Class<? extends Enum<?>> enumType,
      org.jsimpledb.annotation.JField annotation, String description, Method getter, Method setter) {
        super(jdb, name, storageId, TypeToken.of(enumType.asSubclass(Enum.class)),
          new EnumFieldType((Class)enumType), annotation.indexed(), annotation, description, getter, setter);
        this.converter = EnumConverter.createEnumConverter(enumType);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJEnumField(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeToken<? extends Enum<?>> getTypeToken() {
        return (TypeToken<? extends Enum<?>>)this.typeToken;
    }

    @Override
    public Converter<EnumValue, ? extends Enum<?>> getConverter(JTransaction jtx) {
        return this.converter.reverse();
    }

    @Override
    EnumSchemaField toSchemaItem(JSimpleDB jdb) {
        final EnumSchemaField schemaField = new EnumSchemaField();
        this.initialize(jdb, schemaField);
        return schemaField;
    }

    @SuppressWarnings("unchecked")
    void initialize(JSimpleDB jdb, SimpleSchemaField schemaField0) {
        super.initialize(jdb, schemaField0);
        final EnumSchemaField schemaField = (EnumSchemaField)schemaField0;
        schemaField.getIdentifiers().clear();
        for (Enum<?> value : EnumUtil.getValues((Class<Enum<?>>)this.getTypeToken().getRawType()))
            schemaField.getIdentifiers().add(value.name());
    }

    @Override
    JEnumFieldInfo toJFieldInfo(int parentStorageId) {
        return new JEnumFieldInfo(this, parentStorageId);
    }
}

