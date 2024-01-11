
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.CollectionField;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.schema.CollectionSchemaField;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents a collection field in a {@link PermazenClass}.
 */
public abstract class PermazenCollectionField extends PermazenComplexField {

    final PermazenSimpleField elementField;

// Constructor

    PermazenCollectionField(String name, int storageId, Annotation annotation,
      PermazenSimpleField elementField, String description, Method getter) {
        super(name, storageId, annotation, description, getter);
        Preconditions.checkArgument(elementField != null, "null elementField");
        this.elementField = elementField;
    }

// Public Methods

    /**
     * Get the element sub-field.
     *
     * @return this instance's element sub-field
     */
    public PermazenSimpleField getElementField() {
        return this.elementField;
    }

    @Override
    public abstract Collection<?> getValue(PermazenObject pobj);

    @Override
    public List<PermazenSimpleField> getSubFields() {
        return Collections.<PermazenSimpleField>singletonList(this.elementField);
    }

    @Override
    public CollectionField<?, ?> getSchemaItem() {
        return (CollectionField<?, ?>)super.getSchemaItem();
    }

// Package Methods

    @Override
    CollectionSchemaField toSchemaItem() {
        final CollectionSchemaField schemaField = (CollectionSchemaField)super.toSchemaItem();
        schemaField.setElementField(this.elementField.toSchemaItem());
        return schemaField;
    }

    @Override
    abstract CollectionSchemaField createSchemaItem();

    @Override
    public TypeToken<?> getTypeToken() {
        return this.buildTypeToken(this.elementField.getTypeToken().wrap());
    }

    abstract <E> TypeToken<? extends Collection<E>> buildTypeToken(TypeToken<E> elementType);

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType) {
        this.addChangeParameterTypes(types, targetType, this.elementField.getTypeToken());
    }

    abstract <T, E> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType, TypeToken<E> elementType);

// POJO import/export

    @Override
    @SuppressWarnings("unchecked")
    void importPlain(ImportContext context, Object obj, ObjId id) {

        // Get POJO collection
        final Collection<?> objCollection;
        try {
            objCollection = (Collection<?>)obj.getClass().getMethod(this.getter.getName()).invoke(obj);
        } catch (Exception e) {
            return;
        }
        if (objCollection == null)
            return;

        // Get PermazenCollectionField collection
        final Collection<Object> coreCollection = (Collection<Object>)this.readCoreCollection(
          context.getPermazenTransaction().getTransaction(), id);

        // Copy values over
        coreCollection.clear();
        for (Object value : objCollection)
            coreCollection.add(this.elementField.importCoreValue(context, value));
    }

    @Override
    @SuppressWarnings("unchecked")
    void exportPlain(ExportContext context, ObjId id, Object obj) {

        // Get POJO collection
        final Method objGetter;
        try {
            objGetter = obj.getClass().getMethod(this.getter.getName());
        } catch (Exception e) {
            return;
        }
        Collection<Object> objCollection;
        try {
            objCollection = (Collection<Object>)objGetter.invoke(obj);
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to invoke getter method %s for POJO export", objGetter), e);
        }

        // If null, try to create one and identify setter to set it with
        Method objSetter = null;
        if (objCollection == null) {
            try {
                objSetter = Util.findPermazenFieldSetterMethod(obj.getClass(), objGetter);
            } catch (IllegalArgumentException e) {
                return;
            }
            objCollection = this.createPojoCollection(objSetter.getParameterTypes()[0]);
        }

        // Get PermazenCollectionField collection
        final Collection<?> coreCollection = this.readCoreCollection(context.getPermazenTransaction().getTransaction(), id);

        // Copy values over
        objCollection.clear();
        for (Object value : coreCollection)
            objCollection.add(this.elementField.exportCoreValue(context, value));

        // Apply POJO setter if needed
        if (objSetter != null) {
            try {
                objSetter.invoke(obj, objCollection);
            } catch (Exception e) {
                throw new RuntimeException(String.format("failed to invoke setter method %s for POJO export", objSetter), e);
            }
        }
    }

    abstract Collection<?> readCoreCollection(Transaction tx, ObjId id);

    abstract Collection<Object> createPojoCollection(Class<?> collectionType);
}
