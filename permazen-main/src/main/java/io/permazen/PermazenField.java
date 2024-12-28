
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.annotation.PermazenType;
import io.permazen.core.Field;
import io.permazen.core.ObjId;
import io.permazen.core.ObjType;
import io.permazen.core.StaleTransactionException;
import io.permazen.kv.KVDatabase;
import io.permazen.schema.SchemaField;
import io.permazen.util.ByteData;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Represents a field in a Java model object.
 */
public abstract class PermazenField extends PermazenSchemaItem {

    final Method getter;
    final Annotation annotation;

    PermazenSchemaItem parent;                                  // either PermazenClass or PermazenComplexField
    boolean requiresDefaultValidation;

// Constructor

    PermazenField(String name, int storageId, Annotation annotation, String description, Method getter) {
        super(name, storageId, description);
        Preconditions.checkArgument(annotation != null, "null annotation");
        this.annotation = annotation;
        this.getter = getter;
    }

// Public Methods

    /**
     * Get the "full name" of this field.
     *
     * <p>
     * If this field is a sub-field of a complex field, the full name is the field's name qualified
     * by the parent field name, e.g., {@code "mymap.key"}. Otherwise, the full is is the same as the name.
     *
     * @return parent {@link PermazenComplexField}, or null if this instance is not a sub-field
     */
    public String getFullName() {
        return this.name;
    }

    /**
     * Get the getter method associated with this field.
     *
     * @return field property getter method, or null if this field is a sub-field of a complex field
     */
    public Method getGetter() {
        return this.getter;
    }

    /**
     * Get the {@link Annotation} that declared this field.
     *
     * <p>
     * If this field was {@linkplain PermazenType#autogenFields auto-generated} from an abstract
     * method with no annotation, a non-null {@link Annotation} is still returned; it will have all default values.
     *
     * @return declaring annotation
     */
    public Annotation getDeclaringAnnotation() {
        return this.annotation;
    }

    /**
     * Get the Java value of this field in the given Java model object.
     * Does not alter the schema of the object.
     *
     * @param pobj object containing this field
     * @throws io.permazen.core.DeletedObjectException if {@code pobj} does not exist in its associated {@link PermazenTransaction}
     * @throws StaleTransactionException if the {@link PermazenTransaction} associated with {@code pobj} is no longer usable
     * @return this field's value in {@code pobj}
     * @throws IllegalArgumentException if {@code pobj} is null
     * @throws IllegalArgumentException if this field is a sub-field of a complex field
     */
    public abstract Object getValue(PermazenObject pobj);

    /**
     * Get the {@code byte[]} key in the underlying key/value store corresponding to this field in the specified object.
     *
     * <p>
     * Notes:
     * <ul>
     *  <li>Complex fields utilize multiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     *
     * @param pobj Java model object
     * @return the corresonding {@link KVDatabase} key or key prefix
     * @throws IllegalArgumentException if {@code pobj} is null or has the wrong object type
     */
    public ByteData getKey(PermazenObject pobj) {
        Preconditions.checkArgument(pobj != null, "null pobj");
        return this.getSchemaItem().getKey(pobj.getObjId());
    }

    /**
     * Apply visitor pattern.
     *
     * @param target target to invoke
     * @param <R> visit return type
     * @return value from the method of {@code target} corresponding to this instance's type
     * @throws IllegalArgumentException if {@code target} is null
     */
    public abstract <R> R visit(PermazenFieldSwitch<R> target);

    /**
     * Get a {@link Converter} that converts this field's value from what the core database returns
     * to what the Java application expects, or null if no conversion is needed.
     *
     * @param ptx transaction
     * @return {@link Converter} from core API to Java, or null if no conversion is required
     */
    public abstract Converter<?, ?> getConverter(PermazenTransaction ptx);

    /**
     * Get the type of this field.
     *
     * @return this field's type
     */
    public abstract TypeToken<?> getTypeToken();

    /**
     * Get the {@link PermazenClass} of which this field is a member.
     *
     * @return this field's containing object type
     */
    public PermazenClass<?> getPermazenClass() {
        return this.parent instanceof PermazenClass ?
          ((PermazenClass<?>)this.parent) : (PermazenClass<?>)((PermazenComplexField)this.parent).parent;
    }

    @Override
    public Field<?> getSchemaItem() {
        return (Field<?>)super.getSchemaItem();
    }

// Package Methods

    void replaceSchemaItems(ObjType objType) {
        this.schemaItem = objType.getField(this.getFullName());
    }

    /**
     * Determine if this instance and the given field have the same definition.
     *
     * <p>
     * This is used to handle the case where a field is declared via abstract methods in multiple
     * supertypes, but not declared in the model class itself.
     */
    boolean isSameAs(PermazenField that) {
        if (this.getClass() != that.getClass())
            return false;
        if (!Objects.equals(this.name, that.name))
            return false;
        if (this.storageId != that.storageId)
            return false;
        if (!this.description.equals(that.description))
            return false;
        if (!this.getTypeToken().equals(that.getTypeToken()))
            return false;
        if (this.getter != null && that.getter != null && !this.getter.getReturnType().equals(that.getter.getReturnType()))
            return false;
        return true;
    }

    @Override
    SchemaField toSchemaItem() {
        return (SchemaField)super.toSchemaItem();
    }

    @Override
    abstract SchemaField createSchemaItem();

    void calculateRequiresDefaultValidation() {
        this.requiresDefaultValidation = this.getter != null && Util.requiresDefaultValidation(this.getter);
    }

    /**
     * Add the {@link FieldChange} sub-types that are valid parameter types for
     * @OnChange-annotated methods that watch this field as the target field.
     *
     * @param types place to add valid parameter types to
     * @param targetType the type of the class containing the changed field
     * @throws IllegalArgumentException if {@code targetType} does not contain this field
     * @throws UnsupportedOperationException if this field doesn't
     *  {@linkplain #supportsChangeNotifications support change notifications}
     */
    abstract <T> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType);

    boolean supportsChangeNotifications() {
        return true;
    }

// POJO import/export

    abstract void importPlain(ImportContext context, Object obj, ObjId id);

    abstract void exportPlain(ExportContext context, ObjId id, Object obj);

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
          this.getCachedValueFieldName(), Type.getDescriptor(this.getCachedValueEncoding()), null, null);
        valueField.visitEnd();
    }

    void outputCachedNonSimpleValueGetterMethod(ClassGenerator<?> generator, ClassWriter cw, Method fieldReaderMethod) {
        final MethodVisitor mv = generator.startMethod(cw, this.getter);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, generator.getClassName(),
          this.getCachedValueFieldName(), Type.getDescriptor(this.getCachedValueEncoding()));
        mv.visitInsn(Opcodes.DUP);
        final Label valueReady = new Label();
        mv.visitJumpInsn(Opcodes.IFNONNULL, valueReady);
        mv.visitInsn(Opcodes.POP);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, generator.getClassName(),
          ClassGenerator.TX_FIELD_NAME, Type.getDescriptor(PermazenTransaction.class));
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, generator.getClassName(),
          ClassGenerator.ID_FIELD_NAME, Type.getDescriptor(ObjId.class));
        mv.visitLdcInsn(this.name);
        mv.visitInsn(Opcodes.ICONST_1);                                                             // i.e., true
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(PermazenTransaction.class),
          fieldReaderMethod.getName(), Type.getMethodDescriptor(fieldReaderMethod), false);
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(this.getCachedValueEncoding()));
        mv.visitInsn(Opcodes.DUP);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitInsn(Opcodes.SWAP);
        mv.visitFieldInsn(Opcodes.PUTFIELD, generator.getClassName(),
          this.getCachedValueFieldName(), Type.getDescriptor(this.getCachedValueEncoding()));
        mv.visitLabel(valueReady);
        mv.visitFrame(Opcodes.F_SAME1, 0, new Object[0], 1, new String[] { Type.getInternalName(this.getCachedValueEncoding()) });
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    String getCachedValueFieldName() {
        return ClassGenerator.CACHED_VALUE_FIELD_PREFIX + this.name;
    }

    Class<?> getCachedValueEncoding() {
        return this.getter.getReturnType();
    }
}
