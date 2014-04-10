
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.ObjId;
import org.jsimpledb.Transaction;
import org.jsimpledb.jlayer.change.FieldChange;
import org.jsimpledb.jlayer.change.ListFieldAdd;
import org.jsimpledb.jlayer.change.ListFieldChange;
import org.jsimpledb.jlayer.change.ListFieldClear;
import org.jsimpledb.jlayer.change.ListFieldRemove;
import org.jsimpledb.schema.ListSchemaField;
import org.jsimpledb.util.ConvertedList;
import org.objectweb.asm.ClassWriter;

/**
 * Represents a list field in a {@link JClass}.
 */
public class JListField extends JCollectionField {

    private static final int LIST_INDEX_ENTRY_QUERY = 1;

    JListField(String name, int storageId, JSimpleField elementField, String description, Method getter) {
        super(name, storageId, elementField, description, getter);
    }

    @Override
    ListSchemaField toSchemaItem() {
        final ListSchemaField schemaField = new ListSchemaField();
        super.initialize(schemaField);
        return schemaField;
    }

    @Override
    void outputMethods(ClassGenerator<?> generator, ClassWriter cw) {
        this.outputReadMethod(generator, cw, ClassGenerator.READ_LIST_FIELD_METHOD);
    }

    @Override
    void registerChangeListener(Transaction tx, int[] path, AllChangesListener listener) {
        tx.addListFieldChangeListener(this.storageId, path, listener);
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, TypeToken<T> targetType) {
        this.addChangeParameterTypes(types, targetType, this.elementField.typeToken);
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <T, E> void addChangeParameterTypes(List<TypeToken<?>> types, TypeToken<T> targetType, TypeToken<E> elementType) {
        types.add(new TypeToken<FieldChange<T>>() { }
          .where(new TypeParameter<T>() { }, targetType));
        types.add(new TypeToken<ListFieldChange<T, E>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
        types.add(new TypeToken<ListFieldAdd<T, E>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
        types.add(new TypeToken<ListFieldClear<T, E>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
        types.add(new TypeToken<ListFieldRemove<T, E>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
    }

    @Override
    <T> void addIndexEntryReturnTypes(List<TypeToken<?>> types, TypeToken<T> targetType, JSimpleField subField) {
        assert subField == this.elementField;
        this.addIndexEntryReturnTypes(types, targetType, this.elementField.typeToken);
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <T, E> void addIndexEntryReturnTypes(List<TypeToken<?>> types, TypeToken<T> targetType, TypeToken<E> elementType) {
        types.add(new TypeToken<NavigableMap<E, NavigableSet<ListIndexEntry<T>>>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
    }

    @Override
    int getIndexEntryQueryType(TypeToken<?> queryObjectType) {
        return queryObjectType.getRawType().equals(ListIndexEntry.class) ? LIST_INDEX_ENTRY_QUERY : 0;
    }

    @Override
    Method getIndexEntryQueryMethod(int queryType) {
        switch (queryType) {
        case LIST_INDEX_ENTRY_QUERY:
            return ClassGenerator.QUERY_LIST_FIELD_ENTRIES_METHOD;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    List<?> convert(ReferenceConverter converter, Object value) {
        return JListField.convert(converter, value, this.elementField instanceof JReferenceField);
    }

    @SuppressWarnings("unchecked")
    static List<?> convert(ReferenceConverter converter, Object value, boolean refElement) {
        List<?> list = (List<?>)value;
        if (refElement)
            list = new ConvertedList<JObject, ObjId>((List<ObjId>)list, converter);
        return list;
    }
}

