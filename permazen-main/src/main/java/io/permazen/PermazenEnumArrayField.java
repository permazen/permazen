
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.EnumArrayField;
import io.permazen.encoding.Encoding;
import io.permazen.schema.EnumArraySchemaField;

import java.lang.reflect.Method;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

/**
 * Represents an {@link Enum} array field in a {@link PermazenClass}.
 */
public class PermazenEnumArrayField extends AbstractEnumPermazenField<Object, Object> {

    private final int dimensions;

// Constructor

    @SuppressWarnings({ "unchecked", "rawtypes" })
    PermazenEnumArrayField(String name, int storageId, Class<? extends Enum<?>> enumType, Class<?> enumArrayEncoding,
      int dimensions, Encoding<?> encoding, io.permazen.annotation.PermazenField annotation, String description,
      Method getter, Method setter) {
        super(name, storageId, (TypeToken<Object>)TypeToken.of(enumArrayEncoding),
          (Encoding<Object>)encoding, enumType, annotation.indexed(), annotation, description, getter, setter,
          (Converter<Object, Object>)EnumConverter.createEnumArrayConverter(enumType, dimensions));
        Preconditions.checkArgument(dimensions >= 1 && dimensions <= 255, "invalid dimensions");
        this.dimensions = dimensions;
    }

// Public Methods

    @Override
    public <R> R visit(PermazenFieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.casePermazenEnumArrayField(this);
    }

    @Override
    public EnumArrayField getSchemaItem() {
        return (EnumArrayField)super.getSchemaItem();
    }

// Package Methods

    @Override
    EnumArraySchemaField toSchemaItem() {
        final EnumArraySchemaField schemaField = (EnumArraySchemaField)super.toSchemaItem();
        schemaField.setDimensions(this.dimensions);
        return schemaField;
    }

    @Override
    EnumArraySchemaField createSchemaItem() {
        return new EnumArraySchemaField();
    }

    @Override
    void outputCreateConverterBytecode(ClassGenerator<?> generator, MethodVisitor mv) {
        mv.visitLdcInsn(Type.getType(this.enumType));
        mv.visitLdcInsn(this.dimensions);
        generator.emitInvoke(mv, ClassGenerator.ENUM_CONVERTER_CREATE_ARRAY_METHOD);
        generator.emitInvoke(mv, ClassGenerator.CONVERTER_REVERSE_METHOD);
    }
}
