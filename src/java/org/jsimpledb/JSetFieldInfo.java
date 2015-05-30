
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.util.List;
import java.util.NavigableSet;

import org.jsimpledb.change.ListFieldReplace;
import org.jsimpledb.change.SetFieldAdd;
import org.jsimpledb.change.SetFieldClear;
import org.jsimpledb.change.SetFieldRemove;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;

class JSetFieldInfo extends JCollectionFieldInfo {

    JSetFieldInfo(JSetField jfield) {
        super(jfield);
    }

    @Override
    public TypeToken<?> getTypeToken(Class<?> context) {
        return this.buildTypeToken(this.getElementFieldInfo().getTypeToken(context).wrap());
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <E> TypeToken<NavigableSet<E>> buildTypeToken(TypeToken<E> elementType) {
        return new TypeToken<NavigableSet<E>>() { }.where(new TypeParameter<E>() { }, elementType);
    }

    @Override
    void registerChangeListener(Transaction tx, int[] path, Iterable<Integer> types, AllChangesListener listener) {
        tx.addSetFieldChangeListener(this.storageId, path, types, listener);
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType) {
        this.addChangeParameterTypes(types, targetType, this.getElementFieldInfo().getTypeToken(targetType));
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <T, E> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType, TypeToken<E> elementType) {
        types.add(new TypeToken<SetFieldAdd<T, E>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
        types.add(new TypeToken<SetFieldClear<T>>() { }
          .where(new TypeParameter<T>() { }, targetType));
        types.add(new TypeToken<SetFieldRemove<T, E>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
        types.add(new TypeToken<ListFieldReplace<T, E>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<E>() { }, elementType.wrap()));
    }

    @Override
    public NavigableSetConverter<?, ?> getConverter(JTransaction jtx) {
        final Converter<?, ?> elementConverter = this.getElementFieldInfo().getConverter(jtx);
        return elementConverter != null ? this.createConverter(elementConverter) : null;
    }

    // This method exists solely to bind the generic type parameters
    private <X, Y> NavigableSetConverter<X, Y> createConverter(Converter<X, Y> elementConverter) {
        return new NavigableSetConverter<X, Y>(elementConverter);
    }

    @Override
    public void copyRecurse(CopyState copyState, JTransaction srcTx, JTransaction dstTx,
      ObjId id, int storageId, int fieldIndex, int[] fields) {
        assert storageId == this.getElementFieldInfo().storageId;
        this.copyRecurse(copyState, srcTx, dstTx, srcTx.tx.readSetField(id, this.storageId, false), fieldIndex, fields);
    }
}

