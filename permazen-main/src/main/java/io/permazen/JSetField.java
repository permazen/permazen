
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
 * Represents a set field in a {@link JClass}.
 */
public class JSetField extends JCollectionField {

    JSetField(Permazen jdb, String name, int storageId,
      io.permazen.annotation.JSetField annotation, JSimpleField elementField, String description, Method getter) {
        super(jdb, name, storageId, annotation, elementField, description, getter);
    }

    @Override
    public io.permazen.annotation.JSetField getDeclaringAnnotation() {
        return (io.permazen.annotation.JSetField)super.getDeclaringAnnotation();
    }

    @Override
    public NavigableSet<?> getValue(JObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        return jobj.getTransaction().readSetField(jobj.getObjId(), this.storageId, false);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJSetField(this);
    }

    @Override
    SetSchemaField toSchemaItem(Permazen jdb) {
        final SetSchemaField schemaField = new SetSchemaField();
        super.initialize(jdb, schemaField);
        return schemaField;
    }

    @Override
    SetElementIndexInfo toIndexInfo(JSimpleField subField) {
        assert subField == this.elementField;
        return new SetElementIndexInfo(this);
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
    public NavigableSetConverter<?, ?> getConverter(JTransaction jtx) {
        final Converter<?, ?> elementConverter = this.elementField.getConverter(jtx);
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
        return tx.readSetField(id, this.storageId, true);
    }

// Bytecode generation

    @Override
    Method getFieldReaderMethod() {
        return ClassGenerator.JTRANSACTION_READ_SET_FIELD_METHOD;
    }
}

