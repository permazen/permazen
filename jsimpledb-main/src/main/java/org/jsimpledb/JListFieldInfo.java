
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.util.List;

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
    @SuppressWarnings("serial")
    <E> TypeToken<List<E>> buildTypeToken(TypeToken<E> elementType) {
        return new TypeToken<List<E>>() { }.where(new TypeParameter<E>() { }, elementType);
    }

    @Override
    void registerChangeListener(Transaction tx, int[] path, Iterable<Integer> types, AllChangesListener listener) {
        tx.addListFieldChangeListener(this.storageId, path, types, listener);
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
    public ListConverter<?, ?> getConverter(JTransaction jtx) {
        final Converter<?, ?> elementConverter = this.getElementFieldInfo().getConverter(jtx);
        return elementConverter != null ? this.createConverter(elementConverter) : null;
    }

    // This method exists solely to bind the generic type parameters
    private <X, Y> ListConverter<X, Y> createConverter(Converter<X, Y> elementConverter) {
        return new ListConverter<>(elementConverter);
    }

    @Override
    public void copyRecurse(CopyState copyState, JTransaction srcTx, JTransaction dstTx,
      ObjId id, int storageId, int fieldIndex, int[] fields) {
        assert storageId == this.getElementFieldInfo().storageId;
        this.copyRecurse(copyState, srcTx, dstTx, srcTx.tx.readListField(id, this.storageId, false), fieldIndex, fields);
    }
}

