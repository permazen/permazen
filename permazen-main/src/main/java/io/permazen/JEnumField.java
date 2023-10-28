
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.EnumValue;
import io.permazen.core.encoding.EnumValueEncoding;
import io.permazen.schema.EnumSchemaField;

import java.lang.reflect.Method;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

/**
 * Represents an {@link Enum} field in a {@link JClass}.
 */
public class JEnumField extends AbstractEnumJSimpleField<Enum<?>, EnumValue> {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    JEnumField(Permazen jdb, String name, int storageId, Class<? extends Enum<?>> enumType,
      io.permazen.annotation.JField annotation, String description, Method getter, Method setter) {
        super(jdb, name, storageId, (TypeToken<Enum<?>>)TypeToken.of(enumType.asSubclass(Enum.class)),
          new EnumValueEncoding(enumType), enumType, annotation.indexed(), annotation, description, getter, setter,
          (Converter<Enum<?>, EnumValue>)EnumConverter.createEnumConverter(enumType));
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
    EnumSchemaField toSchemaItem(Permazen jdb) {
        final EnumSchemaField schemaField = new EnumSchemaField();
        this.initialize(jdb, schemaField);
        return schemaField;
    }

// Bytecode generation

    @Override
    void outputCreateConverterBytecode(ClassGenerator<?> generator, MethodVisitor mv) {
        mv.visitLdcInsn(Type.getType(this.typeToken.getRawType()));
        generator.emitInvoke(mv, ClassGenerator.ENUM_CONVERTER_CREATE_METHOD);
        generator.emitInvoke(mv, ClassGenerator.CONVERTER_REVERSE_METHOD);
    }
}
