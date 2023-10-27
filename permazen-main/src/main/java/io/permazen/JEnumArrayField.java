
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.Encoding;
import io.permazen.schema.EnumArraySchemaField;
import io.permazen.schema.SimpleSchemaField;

import java.lang.reflect.Method;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

/**
 * Represents an {@link Enum} array field in a {@link JClass}.
 */
public class JEnumArrayField extends AbstractEnumJSimpleField<Object, Object> {

    private final int dimensions;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    JEnumArrayField(Permazen jdb, String name, int storageId, Class<? extends Enum<?>> enumType, Class<?> enumArrayType,
      int dimensions, Encoding<?> encoding, io.permazen.annotation.JField annotation, String description,
      Method getter, Method setter) {
        super(jdb, name, storageId, (TypeToken<Object>)TypeToken.of(enumArrayType), (Encoding<Object>)encoding,
          enumType, annotation.indexed(), annotation, description, getter, setter,
          (Converter<Object, Object>)EnumConverter.createEnumArrayConverter(enumType, dimensions));
        Preconditions.checkArgument(dimensions >= 1 && dimensions <= 255, "invalid dimensions");
        this.dimensions = dimensions;
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJEnumArrayField(this);
    }

    @Override
    EnumArraySchemaField toSchemaItem(Permazen jdb) {
        final EnumArraySchemaField schemaField = new EnumArraySchemaField();
        this.initialize(jdb, schemaField);
        return schemaField;
    }

    @Override
    @SuppressWarnings("unchecked")
    void initialize(Permazen jdb, SimpleSchemaField schemaField0) {
        super.initialize(jdb, schemaField0);
        final EnumArraySchemaField schemaField = (EnumArraySchemaField)schemaField0;
        schemaField.setDimensions(this.dimensions);
    }

// Bytecode generation

    @Override
    void outputCreateConverterBytecode(ClassGenerator<?> generator, MethodVisitor mv) {
        mv.visitLdcInsn(Type.getType(this.enumType));
        mv.visitLdcInsn(this.dimensions);
        generator.emitInvoke(mv, ClassGenerator.ENUM_CONVERTER_CREATE_ARRAY_METHOD);
        generator.emitInvoke(mv, ClassGenerator.CONVERTER_REVERSE_METHOD);
    }
}
