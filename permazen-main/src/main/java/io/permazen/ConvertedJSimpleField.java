
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.FieldType;

import java.lang.reflect.Method;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Support superclass for {@link JSimpleField}'s that require conversion between Java and core API values.
 */
abstract class ConvertedJSimpleField<A, B> extends JSimpleField {

    final Converter<A, B> converter;           // converts Java value -> Core API value in the forward direction

    ConvertedJSimpleField(Permazen jdb, String name, int storageId, TypeToken<A> typeToken, FieldType<B> fieldType,
      boolean indexed, io.permazen.annotation.JField annotation, String description, Method getter, Method setter,
      Converter<A, B> converter) {
        super(jdb, name, storageId, typeToken, fieldType, indexed, annotation, description, getter, setter);
        Preconditions.checkArgument(converter != null, "null converter");
        this.converter = converter;
    }

    @Override
    public Converter<B, A> getConverter(JTransaction jtx) {
        return this.converter.reverse();
    }

    @Override
    boolean isSameAs(JField that0) {
        if (!super.isSameAs(that0))
            return false;
        final ConvertedJSimpleField<?, ?> that = (ConvertedJSimpleField)that0;
        if (!this.converter.equals(that.converter))
            return false;
        return true;
    }

// POJO import/export

    @SuppressWarnings({ "rawtypes", "unchecked" })
    Object importCoreValue(ImportContext context, Object value) {
        return this.converter.convert((A)value);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    Object exportCoreValue(ExportContext context, Object value) {
        return this.converter.reverse().convert((B)value);
    }

// Bytecode generation

    @Override
    void outputFields(ClassGenerator<?> generator, ClassWriter cw) {
        final FieldVisitor valueField = cw.visitField(Opcodes.ACC_PRIVATE | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL,
          ClassGenerator.CONVERTER_FIELD_PREFIX + this.storageId, Type.getDescriptor(Converter.class), null, null);
        valueField.visitEnd();
    }

    @Override
    boolean hasClassInitializerBytecode() {
        return true;
    }

    @Override
    void outputClassInitializerBytecode(ClassGenerator<?> generator, MethodVisitor mv) {
        this.outputCreateConverterBytecode(generator, mv);
        mv.visitFieldInsn(Opcodes.PUTSTATIC, generator.getClassName(),
          ClassGenerator.CONVERTER_FIELD_PREFIX + this.storageId, Type.getDescriptor(Converter.class));
    }

    abstract void outputCreateConverterBytecode(ClassGenerator<?> generator, MethodVisitor mv);

    @Override
    void outputMethods(final ClassGenerator<?> generator, ClassWriter cw) {

        // Getter
        MethodVisitor mv = cw.visitMethod(
          this.getter.getModifiers() & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED),
          this.getter.getName(), Type.getMethodDescriptor(this.getter), null, generator.getExceptionNames(this.getter));
        mv.visitFieldInsn(Opcodes.GETSTATIC, generator.getClassName(),
          ClassGenerator.CONVERTER_FIELD_PREFIX + this.storageId, Type.getDescriptor(Converter.class));
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
          ClassGenerator.CONVERTER_FIELD_PREFIX + this.storageId, Type.getDescriptor(Converter.class));
        generator.emitInvoke(mv, ClassGenerator.CONVERTER_REVERSE_METHOD);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        generator.emitInvoke(mv, ClassGenerator.CONVERTER_CONVERT_METHOD);
        this.outputWriteCoreValueBytecode(generator, mv);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }
}

