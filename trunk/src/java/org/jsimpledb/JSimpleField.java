
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;

import org.dellroad.stuff.java.Primitive;
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
    final String typeName;
    final boolean indexed;
    final Method setter;

    JSimpleField(JSimpleDB jdb, String name, int storageId, TypeToken<?> typeToken,
      String typeName, boolean indexed, String description, Method getter, Method setter) {
        super(jdb, name, storageId, description, getter);
        if (typeName == null)
            throw new IllegalArgumentException("null typeName");
        if (typeToken == null)
            throw new IllegalArgumentException("null typeToken");
        this.typeToken = typeToken;
        this.typeName = typeName;
        this.indexed = indexed;
        this.setter = setter;
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
     */
    public TypeToken<?> getType() {
        return this.typeToken;
    }

    /**
     * Get name of this field's {@link org.jsimpledb.core.FieldType}.
     */
    public String getTypeName() {
        return this.typeName;
    }

    /**
     * Get whether this field is indexed.
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
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        return jobj.getTransaction().readSimpleField(jobj, this.storageId, false);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJSimpleField(this);
    }

    /**
     * Set the Java value of this field in the given object.
     * Does not alter the schema version of the object.
     *
     * @param jtx transaction
     * @param jobj object containing this field
     * @param value new value
     * @throws DeletedObjectException if {@code jobj} does not exist in {@code jtx}
     * @throws StaleTransactionException if {@code jtx} is no longer usable
     * @throws IllegalArgumentException if {@code value} is not an appropriate value for this field
     * @throws IllegalArgumentException if {@code jtx} or {@code jobj} is null
     */
    public void setValue(JTransaction jtx, JObject jobj, Object value) {
        if (jtx == null)
            throw new IllegalArgumentException("null jtx");
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        jtx.writeSimpleField(jobj, this.storageId, value, false);
    }

    @Override
    SimpleSchemaField toSchemaItem(JSimpleDB jdb) {
        final SimpleSchemaField schemaField = new SimpleSchemaField();
        this.initialize(jdb, schemaField);
        return schemaField;
    }

    void initialize(JSimpleDB jdb, SimpleSchemaField schemaField) {
        super.initialize(jdb, schemaField);
        schemaField.setType(this.typeName);
        schemaField.setIndexed(this.indexed);
    }

    @Override
    void outputMethods(final ClassGenerator<?> generator, ClassWriter cw) {

        // Getter
        this.outputReadMethod(generator, cw, ClassGenerator.READ_SIMPLE_FIELD_METHOD);

        // Setter
        final Method writeMethod = ClassGenerator.WRITE_SIMPLE_FIELD_METHOD;
        generator.overrideBeanMethod(cw, this.setter, this.storageId, new ClassGenerator.CodeEmitter() {
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
                  writeMethod.getName(), Type.getMethodDescriptor(writeMethod));
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

