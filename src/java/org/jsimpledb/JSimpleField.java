
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.schema.SimpleSchemaField;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Represents a simple field in a {@link JClass} or a simple sub-field of a complex field in a {@link JClass}.
 */
public class JSimpleField extends JField {

    final TypeToken<?> typeToken;
    final FieldType<?> fieldType;
    final boolean indexed;
    final boolean unique;
    final ArrayList<Object> uniqueExcludes;         // note: these are core API values, sorted by this.fieldType
    final Method setter;

    JSimpleField(JSimpleDB jdb, String name, int storageId, TypeToken<?> typeToken, String typeName, boolean indexed,
      org.jsimpledb.annotation.JField annotation, String description, Method getter, Method setter) {
        this(jdb, name, storageId, typeToken,
          jdb.db.getFieldTypeRegistry().getFieldType(typeName), indexed, annotation, description, getter, setter);
    }

    @SuppressWarnings("unchecked")
    JSimpleField(JSimpleDB jdb, String name, int storageId, TypeToken<?> typeToken, FieldType<?> fieldType, boolean indexed,
      org.jsimpledb.annotation.JField annotation, String description, Method getter, Method setter) {
        super(jdb, name, storageId, description, getter);
        Preconditions.checkArgument(typeToken != null, "null typeToken");
        Preconditions.checkArgument(fieldType != null, "null fieldType");
        Preconditions.checkArgument(annotation != null, "null annotation");
        this.typeToken = typeToken;
        this.fieldType = fieldType;
        this.indexed = indexed;
        this.unique = annotation.unique();
        this.setter = setter;

        // Parse uniqueExcludes
        final int numExcludes = annotation.uniqueExclude().length + (annotation.uniqueExcludeNull() ? 1 : 0);
        if (numExcludes > 0) {
            this.uniqueExcludes = new ArrayList<>(numExcludes);
            if (annotation.uniqueExcludeNull())
                this.uniqueExcludes.add(null);
            for (String string : annotation.uniqueExclude()) {
                try {
                    this.uniqueExcludes.add(this.fieldType.fromString(string));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("invalid uniqueExclude() value `" + string + "': " + e.getMessage(), e);
                }
            }
            Collections.sort(this.uniqueExcludes, (Comparator<Object>)this.fieldType);
        } else
            this.uniqueExcludes = null;
    }

    /**
     * Get the {@link JComplexField} of which this instance is a sub-field, if any.
     *
     * @return parent {@link JComplexField}, or null if this instance is not a sub-field
     */
    public JComplexField getParentField() {
        return this.parent instanceof JComplexField ? (JComplexField)this.parent : null;
    }

    /**
     * Get the type of this field.
     *
     * @return this field's Java type
     */
    public TypeToken<?> getTypeToken() {
        return this.typeToken;
    }

    /**
     * Get the {@link org.jsimpledb.core.FieldType} used by the core API to encode this field's values.
     *
     * <p>
     * Note that for {@link Enum} and reference fields, the core API uses a different type than the Java model
     * classes ({@link org.jsimpledb.core.EnumValue} and {@link org.jsimpledb.core.ObjId}, respectively).
     * Values can always be properly converted using the {@link Converter} returned by {@link #getConverter getConverter()}.
     * </p>
     *
     * @return this field's core-layer type definition
     */
    public FieldType<?> getFieldType() {
        return this.fieldType;
    }

    /**
     * Get whether this field is indexed.
     *
     * @return whether this field is indexed
     */
    public boolean isIndexed() {
        return this.indexed;
    }

    /**
     * Get the setter method associated with this field.
     *
     * @return field property setter method, or null if this field is a sub-field of a complex field
     */
    public Method getSetter() {
        return this.setter;
    }

    @Override
    public Object getValue(JObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        return jobj.getTransaction().readSimpleField(jobj.getObjId(), this.storageId, false);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJSimpleField(this);
    }

    /**
     * Get a {@link Converter} that converts this field values between core API type and Java model type.
     * Only {@link Enum} and reference types require conversion; for all other types, this returns an identity converter.
     *
     * @param jtx transaction
     * @return {@link Converter} from core API field type to Java model field type
     */
    public Converter<?, ?> getConverter(JTransaction jtx) {
        return Converter.<Object>identity();
    }

    /**
     * Set the Java value of this field in the given object.
     * Does not alter the schema version of the object.
     *
     * @param jobj object containing this field
     * @param value new value
     * @throws org.jsimpledb.core.DeletedObjectException if {@code jobj} does not exist in its associated {@link JTransaction}
     * @throws org.jsimpledb.core.StaleTransactionException if the {@link JTransaction} associated with {@code jobj}
     *  is no longer usable
     * @throws IllegalArgumentException if {@code value} is not an appropriate value for this field
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public void setValue(JObject jobj, Object value) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        jobj.getTransaction().writeSimpleField(jobj, this.storageId, value, false);
    }

    @Override
    SimpleSchemaField toSchemaItem(JSimpleDB jdb) {
        final SimpleSchemaField schemaField = new SimpleSchemaField();
        this.initialize(jdb, schemaField);
        return schemaField;
    }

    @Override
    void calculateRequiresValidation() {
        super.calculateRequiresValidation();
        this.requiresValidation |= this.unique;
    }

    void initialize(JSimpleDB jdb, SimpleSchemaField schemaField) {
        super.initialize(jdb, schemaField);
        schemaField.setType(this.fieldType.getName());
        schemaField.setIndexed(this.indexed);
    }

    @Override
    void outputMethods(final ClassGenerator<?> generator, ClassWriter cw) {

        // Getter
        this.outputReadMethod(generator, cw, ClassGenerator.READ_SIMPLE_FIELD_METHOD);

        // Setter
        final Method writeMethod = ClassGenerator.WRITE_SIMPLE_FIELD_METHOD;
        generator.overrideBeanMethod(cw, this.setter, this.storageId, true, new ClassGenerator.CodeEmitter() {
            @Override
            public void emit(MethodVisitor mv) {

                // Push field value
                mv.visitVarInsn(Type.getType(JSimpleField.this.typeToken.getRawType()).getOpcode(Opcodes.ILOAD), 1);

                // Wrap result if needed
                if (JSimpleField.this.typeToken.isPrimitive())
                    generator.wrap(mv, Primitive.get(JSimpleField.this.typeToken.getRawType()));

                // Push "true"
                mv.visitInsn(Opcodes.ICONST_1);

                // Invoke Transaction.writeSimpleField()
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(JTransaction.class),
                  writeMethod.getName(), Type.getMethodDescriptor(writeMethod), false);
            }
        });
    }

    @Override
    final JSimpleFieldInfo toJFieldInfo() {
        return this.toJFieldInfo(0);
    }

    JSimpleFieldInfo toJFieldInfo(int parentStorageId) {
        return new JSimpleFieldInfo(this, parentStorageId);
    }
}

