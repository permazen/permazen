
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.List;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.core.Transaction;
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
    final boolean requiresValidation;

    JSchemaObject parent;

    JField(String name, int storageId, String description, Method getter) {
        super(name, storageId, description);
        this.getter = getter;

        // Check for validation
        this.requiresValidation = getter != null && Util.requiresValidation(getter);
    }

    @Override
    abstract SchemaField toSchemaItem();

    /**
     * Get the getter method associated with this field.
     *
     * @return field property getter method, or null if this field is a sub-field of a complex field
     */
    public Method getGetter() {
        return this.getter;
    }

    /**
     * Determine whether {@linkplain #getGetter this field's getter method} has any JSR 202 annotations.
     *
     * @return true if this field requires validation, otherwise false
     */
    public boolean isRequiresValidation() {
        return this.requiresValidation;
    }

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

                // Invoke JTransaction.readXXX()
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(JTransaction.class),
                  readMethod.getName(), Type.getMethodDescriptor(readMethod));

                // Cast result value
                mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(propertyType.wrap().getRawType()));

                // Unwrap result if necessary
                if (propertyType.isPrimitive())
                    generator.unwrap(mv, Primitive.get(propertyType.getRawType()));
            }
        });
    }

    /**
     * Add the {@link FieldChange} sub-types that are valid parameter types for
     * @OnChange-annotated methods that watch this field as the target field.
     */
    abstract <T> void addChangeParameterTypes(List<TypeToken<?>> types, TypeToken<T> targetType);

    /**
     * Register the given listener as a change listener for this field.
     */
    abstract void registerChangeListener(Transaction tx, int[] path, AllChangesListener listener);

    /**
     * Convert this field's value so that {@link org.jsimpledb.ObjId}s become {@link JObject}s as needed.
     */
    abstract Object convert(ReferenceConverter converter, Object value);
}

