
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.schema.ComplexSchemaField;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;

import org.objectweb.asm.ClassWriter;

/**
 * Represents a complex field in a {@link JClass}.
 */
public abstract class JComplexField extends JField {

    JComplexField(Permazen jdb, String name, int storageId, Annotation annotation, String description, Method getter) {
        super(jdb, name, storageId, annotation, description, getter);
        Preconditions.checkArgument(name != null, "null name");
    }

    @Override
    abstract ComplexSchemaField toSchemaItem(Permazen jdb);

    /**
     * Get the sub-fields associated with this field.
     *
     * @return this instance's sub-fields (in order)
     */
    public abstract List<JSimpleField> getSubFields();

    /**
     * Get the sub-field with the given name.
     *
     * @param name sub-field name
     * @return the sub-field with the specified name
     * @throws IllegalArgumentException if {@code name} is invalid
     */
    public abstract JSimpleField getSubField(String name);

    /**
     * Get the sub-field with the given storage ID.
     *
     * @param storageId sub-field storage ID
     * @throws IllegalArgumentException if not found
     */
    JSimpleField getSubField(int storageId) {
        for (JSimpleField subField : this.getSubFields()) {
            if (subField.storageId == storageId)
                return subField;
        }
        throw new IllegalArgumentException("storage ID " + storageId + " not found");
    }

    /**
     * Get the name of the given sub-field.
     *
     * @throws IllegalArgumentException if {@code subField} is not one of {@link #getSubFields}
     */
    abstract String getSubFieldName(JSimpleField subField);

    abstract SimpleFieldIndexInfo toIndexInfo(JSimpleField subField);

    @Override
    JClass<?> getJClass() {
        assert this.parent instanceof JClass;
        return (JClass<?>)this.parent;
    }

    @Override
    boolean supportsChangeNotifications() {
        return true;
    }

// Bytecode generation

    @Override
    final void outputFields(ClassGenerator<?> generator, ClassWriter cw) {
        this.outputCachedValueField(generator, cw);
    }

    @Override
    final void outputMethods(ClassGenerator<?> generator, ClassWriter cw) {
        this.outputCachedNonSimpleValueGetterMethod(generator, cw, this.getFieldReaderMethod());
    }

    abstract Method getFieldReaderMethod();
}

