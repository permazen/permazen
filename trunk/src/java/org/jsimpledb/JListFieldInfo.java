
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.change.ListFieldAdd;
import org.jsimpledb.change.ListFieldClear;
import org.jsimpledb.change.ListFieldRemove;
import org.jsimpledb.change.ListFieldReplace;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;

class JListFieldInfo extends JCollectionFieldInfo {

    private static final int LIST_INDEX_ENTRY_QUERY = 1;

    JListFieldInfo(JListField jfield) {
        super(jfield);
    }

    @Override
    public TypeToken<?> getTypeToken() {
        return this.buildTypeToken(this.getElementFieldInfo().getTypeToken().wrap());
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <E> TypeToken<List<E>> buildTypeToken(TypeToken<E> elementType) {
        return new TypeToken<List<E>>() { }.where(new TypeParameter<E>() { }, elementType);
    }

    @Override
    void registerChangeListener(Transaction tx, int[] path, Iterable<Integer> types, AllChangesListener listener) {
        tx.addListFieldChangeListener(this.storageId, path, types, listener);
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, TypeToken<T> targetType) {
        this.addChangeParameterTypes(types, targetType, this.getElementFieldInfo().getTypeToken());
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <T, E> void addChangeParameterTypes(List<TypeToken<?>> types, TypeToken<T> targetType, TypeToken<E> elementType) {
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
    @SuppressWarnings("serial")
    <T, E> void addIndexEntryReturnTypes(List<TypeToken<?>> types,
      TypeToken<T> targetType, JSimpleFieldInfo subFieldInfo, TypeToken<E> elementType) {
        assert subFieldInfo == this.getElementFieldInfo();
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
    public ListConverter<?, ?> getConverter(JTransaction jtx) {
        final Converter<?, ?> elementConverter = this.getElementFieldInfo().getConverter(jtx);
        return elementConverter != null ? this.createConverter(elementConverter) : null;
    }

    // This method exists solely to bind the generic type parameters
    private <X, Y> ListConverter<X, Y> createConverter(Converter<X, Y> elementConverter) {
        return new ListConverter<X, Y>(elementConverter);
    }

    @Override
    public void copyRecurse(ObjIdSet seen, JTransaction srcTx, JTransaction dstTx,
      ObjId id, int storageId, Deque<Integer> nextFields) {
        if (storageId != this.getElementFieldInfo().storageId)
            throw new RuntimeException("internal error");
        this.copyRecurse(seen, srcTx, dstTx, srcTx.tx.readListField(id, this.storageId, false), nextFields);
    }
}

