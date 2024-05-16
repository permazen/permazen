
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.change.ListFieldAdd;
import io.permazen.change.ListFieldClear;
import io.permazen.change.ListFieldRemove;
import io.permazen.change.ListFieldReplace;
import io.permazen.core.ListField;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.schema.ListSchemaField;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a list field in a {@link PermazenClass}.
 */
public class PermazenListField extends PermazenCollectionField {

// Constructor

    PermazenListField(String name, int storageId, io.permazen.annotation.PermazenListField annotation,
      PermazenSimpleField elementField, String description, Method getter) {
        super(name, storageId, annotation, elementField, description, getter);
    }

// Public Methods

    @Override
    public io.permazen.annotation.PermazenListField getDeclaringAnnotation() {
        return (io.permazen.annotation.PermazenListField)super.getDeclaringAnnotation();
    }

    @Override
    public List<?> getValue(PermazenObject pobj) {
        Preconditions.checkArgument(pobj != null, "null pobj");
        return pobj.getPermazenTransaction().readListField(pobj.getObjId(), this.name, false);
    }

    @Override
    public <R> R visit(PermazenFieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.casePermazenListField(this);
    }

    @Override
    public ListField<?> getSchemaItem() {
        return (ListField<?>)super.getSchemaItem();
    }

// Package Methods

    @Override
    ListSchemaField createSchemaItem() {
        return new ListSchemaField();
    }

    @Override
    @SuppressWarnings("unchecked")
    Iterable<ObjId> iterateReferences(Transaction tx, ObjId id, PermazenReferenceField subField) {
        return (Iterable<ObjId>)tx.readListField(id, this.name, false);
    }

    @Override
    @SuppressWarnings("serial")
    <E> TypeToken<List<E>> buildTypeToken(TypeToken<E> elementType) {
        return new TypeToken<List<E>>() { }.where(new TypeParameter<E>() { }, elementType);
    }

    // This method exists solely to bind the generic type parameters
    @Override
    @SuppressWarnings("serial")
    <T, E> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType, TypeToken<E> elementType) {
        types.add(new TypeToken<ListFieldAdd<T, E>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
        types.add(new TypeToken<ListFieldClear<T>>() { }
          .where(new TypeParameter<T>() { }, targetType));
        types.add(new TypeToken<ListFieldRemove<T, E>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
        types.add(new TypeToken<ListFieldReplace<T, E>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
    }

    @Override
    public ListConverter<?, ?> getConverter(PermazenTransaction ptx) {
        final Converter<?, ?> elementConverter = this.elementField.getConverter(ptx);
        return elementConverter != null ? this.createConverter(elementConverter) : null;
    }

    // This method exists solely to bind the generic type parameters
    private <X, Y> ListConverter<X, Y> createConverter(Converter<X, Y> elementConverter) {
        return new ListConverter<>(elementConverter);
    }

// POJO import/export

    @Override
    List<Object> createPojoCollection(Class<?> collectionType) {
        return new ArrayList<>();
    }

    @Override
    List<?> readCoreCollection(Transaction tx, ObjId id) {
        return tx.readListField(id, this.name, true);
    }

// Bytecode generation

    @Override
    Method getFieldReaderMethod() {
        return ClassGenerator.PERMAZEN_TRANSACTION_READ_LIST_FIELD_METHOD;
    }
}
