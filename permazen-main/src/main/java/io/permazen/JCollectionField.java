
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
 * Represents a collection field in a {@link JClass}.
 */
public abstract class JCollectionField extends JComplexField {

    final JSimpleField elementField;

    JCollectionField(Permazen jdb, String name, int storageId,
      Annotation annotation, JSimpleField elementField, String description, Method getter) {
        super(jdb, name, storageId, annotation, description, getter);
        Preconditions.checkArgument(elementField != null, "null elementField");
        this.elementField = elementField;
    }

    /**
     * Get the element sub-field.
     *
     * @return this instance's element sub-field
     */
    public JSimpleField getElementField() {
        return this.elementField;
    }

    @Override
    public abstract Collection<?> getValue(JObject jobj);

    @Override
    public List<JSimpleField> getSubFields() {
        return Collections.<JSimpleField>singletonList(this.elementField);
    }

    @Override
    boolean isSameAs(JField that0) {
        if (!super.isSameAs(that0))
            return false;
        final JCollectionField that = (JCollectionField)that0;
        if (!this.elementField.isSameAs(that.elementField))
            return false;
        return true;
    }

    @Override
    String getSubFieldName(JSimpleField subField) {
        if (subField == this.elementField)
            return CollectionField.ELEMENT_FIELD_NAME;
        throw new IllegalArgumentException("unknown sub-field");
    }

    @Override
    abstract CollectionSchemaField toSchemaItem(Permazen jdb);

    void initialize(Permazen jdb, CollectionSchemaField schemaField) {
        super.initialize(jdb, schemaField);
        schemaField.setElementField(this.elementField.toSchemaItem(jdb));
    }

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

        // Get JCollectionField collection
        final Collection<Object> coreCollection = (Collection<Object>)this.readCoreCollection(
          context.getTransaction().getTransaction(), id);

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
                objSetter = Util.findJFieldSetterMethod(obj.getClass(), objGetter);
            } catch (IllegalArgumentException e) {
                return;
            }
            objCollection = this.createPojoCollection(objSetter.getParameterTypes()[0]);
        }

        // Get JCollectionField collection
        final Collection<?> coreCollection = this.readCoreCollection(context.getTransaction().getTransaction(), id);

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
