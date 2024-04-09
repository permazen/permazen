
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.core.ComplexField;
import io.permazen.core.ObjId;
import io.permazen.core.ObjType;
import io.permazen.core.Transaction;
import io.permazen.schema.ComplexSchemaField;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.objectweb.asm.ClassWriter;

/**
 * Represents a complex field in a {@link PermazenClass}.
 */
public abstract class PermazenComplexField extends PermazenField {

// Constructor

    PermazenComplexField(String name, int storageId, Annotation annotation, String description, Method getter) {
        super(name, storageId, annotation, description, getter);
        Preconditions.checkArgument(name != null, "null name");
    }

// Public Methods

    /**
     * Get the sub-fields associated with this field.
     *
     * @return this instance's sub-fields (in order)
     */
    public abstract List<PermazenSimpleField> getSubFields();

    /**
     * Get the sub-field with the given name.
     *
     * @param name sub-field name
     * @return the sub-field with the specified name
     * @throws IllegalArgumentException if {@code name} is invalid
     */
    public final PermazenSimpleField getSubField(String name) {

        // Sanity check
        Preconditions.checkArgument(name != null, "null name");

        // Find sub-fields
        for (PermazenSimpleField subField : this.getSubFields()) {
            if (name.equals(subField.name))
                return subField;
        }

        // Build helpful error message
        final String hints = this.getSubFields().stream()
          .map(PermazenSimpleField::getName)
          .map(fieldName -> '"' + fieldName + '"')
          .collect(Collectors.joining(" or "));
        throw new IllegalArgumentException(String.format("unknown sub-field \"%s\" (did you mean %s instead?)", name, hints));
    }

    @Override
    public ComplexField<?> getSchemaItem() {
        return (ComplexField<?>)super.getSchemaItem();
    }

// Package Methods

    void replaceSchemaItems(ObjType objType) {
        super.replaceSchemaItems(objType);
        for (PermazenSimpleField subField : this.getSubFields())
            subField.replaceSchemaItems(objType);
    }

    @Override
    boolean isSameAs(PermazenField that0) {
        if (!super.isSameAs(that0))
            return false;
        final PermazenComplexField that = (PermazenComplexField)that0;
        final List<PermazenSimpleField> thisSubFields = this.getSubFields();
        final List<PermazenSimpleField> thatSubFields = that.getSubFields();
        if (thisSubFields.size() != thatSubFields.size())
            return false;
        for (int i = 0; i < thisSubFields.size(); i++) {
            if (!thisSubFields.get(i).isSameAs(thatSubFields.get(i)))
                return false;
        }
        return true;
    }

    @Override
    ComplexSchemaField toSchemaItem() {
        return (ComplexSchemaField)super.toSchemaItem();
    }

    @Override
    abstract ComplexSchemaField createSchemaItem();

    @Override
    void visitSchemaItems(Consumer<? super PermazenSchemaItem> visitor) {
        super.visitSchemaItems(visitor);
        this.getSubFields().forEach(item -> item.visitSchemaItems(visitor));
    }

    // Iterate all of the values in the given reference sub-field in the given object
    abstract Iterable<ObjId> iterateReferences(Transaction tx, ObjId id, PermazenReferenceField subField);

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
