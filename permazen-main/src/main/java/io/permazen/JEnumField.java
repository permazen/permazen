
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;

import org.dellroad.stuff.java.EnumUtil;
import io.permazen.core.EnumValue;
import io.permazen.core.type.EnumFieldType;
import io.permazen.schema.EnumSchemaField;
import io.permazen.schema.SimpleSchemaField;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Represents an {@link Enum} field in a {@link JClass}.
 */
public class JEnumField extends JSimpleField {

    final EnumConverter<?> converter;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    JEnumField(JSimpleDB jdb, String name, int storageId, Class<? extends Enum<?>> enumType,
      io.permazen.annotation.JField annotation, String description, Method getter, Method setter) {
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
    boolean isSameAs(JField that0) {
        if (!super.isSameAs(that0))
            return false;
        final JEnumField that = (JEnumField)that0;
        if (!this.converter.equals(that.converter))
            return false;
        return true;
    }

    @Override
    EnumSchemaField toSchemaItem(JSimpleDB jdb) {
        final EnumSchemaField schemaField = new EnumSchemaField();
        this.initialize(jdb, schemaField);
        return schemaField;
    }

    @Override
    @SuppressWarnings("unchecked")
    void initialize(JSimpleDB jdb, SimpleSchemaField schemaField0) {
        super.initialize(jdb, schemaField0);
        final EnumSchemaField schemaField = (EnumSchemaField)schemaField0;
        schemaField.setType(null);                                          // core API ignores "type" of Enum fields
        schemaField.getIdentifiers().clear();
        for (Enum<?> value : EnumUtil.getValues((Class<Enum<?>>)this.getTypeToken().getRawType()))
            schemaField.getIdentifiers().add(value.name());
    }

// Bytecode generation

    @Override
    void outputFields(ClassGenerator<?> generator, ClassWriter cw) {
        final FieldVisitor valueField = cw.visitField(Opcodes.ACC_PRIVATE | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL,
          ClassGenerator.ENUM_CONVERTER_FIELD_PREFIX + this.storageId, Type.getDescriptor(Converter.class), null, null);
        valueField.visitEnd();
    }

    @Override
    boolean hasClassInitializerBytecode() {
        return true;
    }

    @Override
    void outputClassInitializerBytecode(ClassGenerator<?> generator, MethodVisitor mv) {
        mv.visitLdcInsn(Type.getType(this.typeToken.getRawType()));
        generator.emitInvoke(mv, ClassGenerator.ENUM_CONVERTER_CREATE_METHOD);
        generator.emitInvoke(mv, ClassGenerator.CONVERTER_REVERSE_METHOD);
        mv.visitFieldInsn(Opcodes.PUTSTATIC, generator.getClassName(),
          ClassGenerator.ENUM_CONVERTER_FIELD_PREFIX + this.storageId, Type.getDescriptor(Converter.class));
    }

    @Override
    void outputMethods(final ClassGenerator<?> generator, ClassWriter cw) {

        // Getter
        MethodVisitor mv = cw.visitMethod(
          this.getter.getModifiers() & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED),
          this.getter.getName(), Type.getMethodDescriptor(this.getter), null, generator.getExceptionNames(this.getter));
        mv.visitFieldInsn(Opcodes.GETSTATIC, generator.getClassName(),
          ClassGenerator.ENUM_CONVERTER_FIELD_PREFIX + this.storageId, Type.getDescriptor(Converter.class));
        this.outputReadCoreValueBytecode(generator, mv);
        generator.emitInvoke(mv, ClassGenerator.CONVERTER_CONVERT_METHOD);
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(this.getter.getReturnType()));
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Setter
        mv = cw.visitMethod(
          this.setter.getModifiers() & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED),
          this.setter.getName(), Type.getMethodDescriptor(this.setter), null, generator.getExceptionNames(this.setter));
        mv.visitFieldInsn(Opcodes.GETSTATIC, generator.getClassName(),
          ClassGenerator.ENUM_CONVERTER_FIELD_PREFIX + this.storageId, Type.getDescriptor(Converter.class));
        generator.emitInvoke(mv, ClassGenerator.CONVERTER_REVERSE_METHOD);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        generator.emitInvoke(mv, ClassGenerator.CONVERTER_CONVERT_METHOD);
        this.outputWriteCoreValueBytecode(generator, mv);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }
}

