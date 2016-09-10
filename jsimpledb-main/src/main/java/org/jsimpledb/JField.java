
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.schema.SchemaField;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Represents a field in a Java model object.
 */
public abstract class JField extends JSchemaObject {

    final Method getter;

    JSchemaObject parent;                               // either JClass or JComplexField
    boolean requiresDefaultValidation;

    JField(JSimpleDB jdb, String name, int storageId, String description, Method getter) {
        super(jdb, name, storageId, description);
        this.getter = getter;
    }

    void calculateRequiresDefaultValidation() {
        this.requiresDefaultValidation = this.getter != null && Util.requiresDefaultValidation(this.getter);
    }

    /**
     * Determine if this instance and the given instance are effectively the same.
     *
     * <p>
     * This is used to handle the case where a field is declared via abstract methods in multiple
     * supertypes, but not declared in the model class itself.
     */
    boolean isSameAs(JField that) {
        if (!(this.name != null ? this.name.equals(that.name) : that.name == null))
            return false;
        if (this.storageId != that.storageId)
            return false;
        if (!this.description.equals(that.description))
            return false;
        if (!this.getClass().equals(that.getClass()))
            return false;
        if (!this.getter.getReturnType().equals(that.getter.getReturnType()))
            return false;
        if (!Arrays.equals(this.getter.getParameterTypes(), that.getter.getParameterTypes()))
            return false;
        return true;
    }

    @Override
    abstract SchemaField toSchemaItem(JSimpleDB jdb);

    /**
     * Get the getter method associated with this field.
     *
     * @return field property getter method, or null if this field is a sub-field of a complex field
     */
    public Method getGetter() {
        return this.getter;
    }

    /**
     * Get the Java value of this field in the given Java model object.
     * Does not alter the schema version of the object.
     *
     * @param jobj object containing this field
     * @throws org.jsimpledb.core.DeletedObjectException if {@code jobj} does not exist in its associated {@link JTransaction}
     * @throws org.jsimpledb.kv.StaleTransactionException if the {@link JTransaction} associated with {@code jobj}
     *  is no longer usable
     * @return this field's value in {@code jobj}
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public abstract Object getValue(JObject jobj);

    /**
     * Apply visitor pattern.
     *
     * @param target target to invoke
     * @param <R> visit return type
     * @return value from the method of {@code target} corresponding to this instance's type
     * @throws NullPointerException if {@code target} is null
     */
    public abstract <R> R visit(JFieldSwitch<R> target);

    /**
     * Create a {@link JFieldInfo} instance that corresponds to this instance.
     */
    abstract JFieldInfo toJFieldInfo();

// Bytecode generation

    boolean hasClassInitializerBytecode() {
        return false;
    }

    void outputClassInitializerBytecode(ClassGenerator<?> generator, MethodVisitor mv) {
        throw new UnsupportedOperationException();
    }

    void outputFields(ClassGenerator<?> generator, ClassWriter cw) {
    }

    abstract void outputMethods(ClassGenerator<?> generator, ClassWriter cw);

    void outputCachedValueField(ClassGenerator<?> generator, ClassWriter cw) {
        final FieldVisitor valueField = cw.visitField(Opcodes.ACC_PRIVATE,
          this.getCachedValueFieldName(), Type.getDescriptor(this.getCachedValueFieldType()), null, null);
        valueField.visitEnd();
    }

    void outputCachedNonSimpleValueGetterMethod(ClassGenerator<?> generator, ClassWriter cw, Method fieldReaderMethod) {
        final MethodVisitor mv = generator.startMethod(cw, this.getter);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, generator.getClassName(),
          this.getCachedValueFieldName(), Type.getDescriptor(this.getCachedValueFieldType()));
        mv.visitInsn(Opcodes.DUP);
        final Label valueReady = new Label();
        mv.visitJumpInsn(Opcodes.IFNONNULL, valueReady);
        mv.visitInsn(Opcodes.POP);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, generator.getClassName(),
          ClassGenerator.TX_FIELD_NAME, Type.getDescriptor(JTransaction.class));
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, generator.getClassName(),
          ClassGenerator.ID_FIELD_NAME, Type.getDescriptor(ObjId.class));
        mv.visitLdcInsn(this.storageId);
        mv.visitInsn(Opcodes.ICONST_1);                                                             // i.e., true
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(JTransaction.class),
          fieldReaderMethod.getName(), Type.getMethodDescriptor(fieldReaderMethod), false);
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(this.getCachedValueFieldType()));
        mv.visitInsn(Opcodes.DUP);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitInsn(Opcodes.SWAP);
        mv.visitFieldInsn(Opcodes.PUTFIELD, generator.getClassName(),
          this.getCachedValueFieldName(), Type.getDescriptor(this.getCachedValueFieldType()));
        mv.visitLabel(valueReady);
        mv.visitFrame(Opcodes.F_SAME1, 0, new Object[0], 1, new String[] { Type.getInternalName(this.getCachedValueFieldType()) });
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    String getCachedValueFieldName() {
        return ClassGenerator.JFIELD_FIELD_PREFIX + this.storageId;
    }

    Class<?> getCachedValueFieldType() {
        return this.getter.getReturnType();
    }
}

