
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.change.SetFieldAdd;
import io.permazen.change.SetFieldClear;
import io.permazen.change.SetFieldRemove;
import io.permazen.core.ObjId;
import io.permazen.core.SetField;
import io.permazen.core.Transaction;
import io.permazen.schema.SetSchemaField;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Represents a set field in a {@link PermazenClass}.
 */
public class PermazenSetField extends PermazenCollectionField {

// Constructor

    PermazenSetField(String name, int storageId, io.permazen.annotation.PermazenSetField annotation,
      PermazenSimpleField elementField, String description, Method getter) {
        super(name, storageId, annotation, elementField, description, getter);
    }

// Public Methods

    @Override
    public io.permazen.annotation.PermazenSetField getDeclaringAnnotation() {
        return (io.permazen.annotation.PermazenSetField)super.getDeclaringAnnotation();
    }

    @Override
    public NavigableSet<?> getValue(PermazenObject pobj) {
        Preconditions.checkArgument(pobj != null, "null pobj");
        return pobj.getPermazenTransaction().readSetField(pobj.getObjId(), this.name, false);
    }

    @Override
    public <R> R visit(PermazenFieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.casePermazenSetField(this);
    }

    @Override
    public SetField<?> getSchemaItem() {
        return (SetField<?>)super.getSchemaItem();
    }

// Package Methods

    @Override
    SetSchemaField createSchemaItem() {
        return new SetSchemaField();
    }

    @Override
    @SuppressWarnings("unchecked")
    Iterable<ObjId> iterateReferences(Transaction tx, ObjId id, PermazenReferenceField subField) {
        return (Iterable<ObjId>)tx.readSetField(id, this.name, false);
    }

    @Override
    @SuppressWarnings("serial")
    <E> TypeToken<NavigableSet<E>> buildTypeToken(TypeToken<E> elementType) {
        return new TypeToken<NavigableSet<E>>() { }.where(new TypeParameter<E>() { }, elementType);
    }

    // This method exists solely to bind the generic type parameters
    @Override
    @SuppressWarnings("serial")
    <T, E> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType, TypeToken<E> elementType) {
        types.add(new TypeToken<SetFieldAdd<T, E>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
        types.add(new TypeToken<SetFieldClear<T>>() { }
          .where(new TypeParameter<T>() { }, targetType));
        types.add(new TypeToken<SetFieldRemove<T, E>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
    }

    @Override
    public NavigableSetConverter<?, ?> getConverter(PermazenTransaction ptx) {
        final Converter<?, ?> elementConverter = this.elementField.getConverter(ptx);
        return elementConverter != null ? this.createConverter(elementConverter) : null;
    }

    // This method exists solely to bind the generic type parameters
    private <X, Y> NavigableSetConverter<X, Y> createConverter(Converter<X, Y> elementConverter) {
        return new NavigableSetConverter<>(elementConverter);
    }

// POJO import/export

    @Override
    Set<Object> createPojoCollection(Class<?> collectionType) {
        return ConcurrentSkipListSet.class.isAssignableFrom(collectionType) ? new ConcurrentSkipListSet<Object>() :
          NavigableSet.class.isAssignableFrom(collectionType) ? new TreeSet<Object>() : new HashSet<Object>();
    }

    @Override
    NavigableSet<?> readCoreCollection(Transaction tx, ObjId id) {
        return tx.readSetField(id, this.name, true);
    }

// Bytecode generation

    @Override
    Method getFieldReaderMethod() {
        return ClassGenerator.PERMAZEN_TRANSACTION_READ_SET_FIELD_METHOD;
    }
}
