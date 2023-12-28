
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.EnumField;
import io.permazen.core.EnumValue;
import io.permazen.core.EnumValueEncoding;
import io.permazen.schema.EnumSchemaField;

import java.lang.reflect.Method;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

/**
 * Represents an {@link Enum} field in a {@link JClass}.
 */
public class JEnumField extends AbstractEnumJField<Enum<?>, EnumValue> {

// Constructor

    @SuppressWarnings({ "unchecked", "rawtypes" })
    JEnumField(String name, int storageId, Class<? extends Enum<?>> enumType,
      io.permazen.annotation.JField annotation, String description, Method getter, Method setter) {
        super(name, storageId, (TypeToken<Enum<?>>)TypeToken.of(enumType.asSubclass(Enum.class)),
          new EnumValueEncoding(enumType), enumType, annotation.indexed(), annotation, description, getter, setter,
          (Converter<Enum<?>, EnumValue>)EnumConverter.createEnumConverter(enumType));
    }

// Public Methods

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseJEnumField(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeToken<? extends Enum<?>> getTypeToken() {
        return (TypeToken<? extends Enum<?>>)this.typeToken;
    }

    @Override
    public EnumField getSchemaItem() {
        return (EnumField)super.getSchemaItem();
    }

// Package Methods

    @Override
    EnumSchemaField createSchemaItem() {
        return new EnumSchemaField();
    }

    @Override
    void outputCreateConverterBytecode(ClassGenerator<?> generator, MethodVisitor mv) {
        mv.visitLdcInsn(Type.getType(this.typeToken.getRawType()));
        generator.emitInvoke(mv, ClassGenerator.ENUM_CONVERTER_CREATE_METHOD);
        generator.emitInvoke(mv, ClassGenerator.CONVERTER_REVERSE_METHOD);
    }
}
