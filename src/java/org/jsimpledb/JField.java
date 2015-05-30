
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.schema.SchemaField;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Represents a field in a Java model object.
 */
public abstract class JField extends JSchemaObject {

    final Method getter;

    JSchemaObject parent;
    boolean requiresValidation;

    JField(JSimpleDB jdb, String name, int storageId, String description, Method getter) {
        super(jdb, name, storageId, description);
        this.getter = getter;
    }

    void calculateRequiresValidation() {
        this.requiresValidation = this.getter != null && Util.requiresValidation(this.getter);
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

    abstract void outputMethods(ClassGenerator<?> generator, ClassWriter cw);

    /**
     * Generate bytecode for getter method override.
     */
    void outputReadMethod(final ClassGenerator<?> generator, ClassWriter cw, final Method readMethod) {

        // Get property type
        final TypeToken<?> propertyType = TypeToken.of(this.getter.getReturnType());

        // Generate method override
        generator.overrideBeanMethod(cw, this.getter, this.storageId, new ClassGenerator.CodeEmitter() {
            @Override
            public void emit(MethodVisitor mv) {

                // Push "true"
                mv.visitInsn(Opcodes.ICONST_1);

                // Invoke JTransaction.readXXX()
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(JTransaction.class),
                  readMethod.getName(), Type.getMethodDescriptor(readMethod), false);

                // Cast result value
                mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(propertyType.wrap().getRawType()));

                // Unwrap result if necessary
                if (propertyType.isPrimitive())
                    generator.unwrap(mv, Primitive.get(propertyType.getRawType()));
            }
        });
    }

    /**
     * Create a {@link JFieldInfo} instance that corresponds to this instance.
     */
    abstract JFieldInfo toJFieldInfo();
}

