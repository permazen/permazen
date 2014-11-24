
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

import org.jsimpledb.change.MapFieldAdd;
import org.jsimpledb.change.MapFieldClear;
import org.jsimpledb.change.MapFieldRemove;
import org.jsimpledb.change.MapFieldReplace;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;

class JMapFieldInfo extends JComplexFieldInfo {

    private static final int VALUE_INDEX_ENTRY_QUERY = 1;

    JMapFieldInfo(JMapField jfield) {
        super(jfield);
    }

    /**
     * Get the key sub-field info.
     */
    public JSimpleFieldInfo getKeyFieldInfo() {
        return this.getSubFieldInfos().get(0);
    }

    /**
     * Get the value sub-field info.
     */
    public JSimpleFieldInfo getValueFieldInfo() {
        return this.getSubFieldInfos().get(1);
    }

    @Override
    public String getSubFieldInfoName(JSimpleFieldInfo subFieldInfo) {
        if (subFieldInfo.getStorageId() == this.getKeyFieldInfo().getStorageId())
            return MapField.KEY_FIELD_NAME;
        if (subFieldInfo.getStorageId() == this.getValueFieldInfo().getStorageId())
            return MapField.VALUE_FIELD_NAME;
        throw new RuntimeException("internal error");
    }

    @Override
    public TypeToken<?> getTypeToken() {
        return this.buildTypeToken(this.getKeyFieldInfo().getTypeToken().wrap(), this.getValueFieldInfo().getTypeToken().wrap());
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <K, V> TypeToken<NavigableMap<K, V>> buildTypeToken(TypeToken<K> keyType, TypeToken<V> valueType) {
        return new TypeToken<NavigableMap<K, V>>() { }
          .where(new TypeParameter<K>() { }, keyType)
          .where(new TypeParameter<V>() { }, valueType);
    }

    @Override
    void registerChangeListener(Transaction tx, int[] path, Iterable<Integer> types, AllChangesListener listener) {
        tx.addMapFieldChangeListener(this.storageId, path, types, listener);
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, TypeToken<T> targetType) {
        this.addChangeParameterTypes(types, targetType,
          this.getKeyFieldInfo().getTypeToken(), this.getValueFieldInfo().getTypeToken());
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <T, K, V> void addChangeParameterTypes(List<TypeToken<?>> types,
      TypeToken<T> targetType, TypeToken<K> keyType, TypeToken<V> valueType) {
        types.add(new TypeToken<MapFieldAdd<T, K, V>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<K>() { }, keyType.wrap())
          .where(new TypeParameter<V>() { }, valueType.wrap()));
        types.add(new TypeToken<MapFieldClear<T>>() { }
          .where(new TypeParameter<T>() { }, targetType));
        types.add(new TypeToken<MapFieldRemove<T, K, V>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<K>() { }, keyType.wrap())
          .where(new TypeParameter<V>() { }, valueType.wrap()));
        types.add(new TypeToken<MapFieldReplace<T, K, V>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<K>() { }, keyType.wrap())
          .where(new TypeParameter<V>() { }, valueType.wrap()));
    }

    @Override
    <T, R> void addIndexEntryReturnTypes(List<TypeToken<?>> types,
      TypeToken<T> targetType, JSimpleFieldInfo subFieldInfo, TypeToken<R> valueType) {
        if (subFieldInfo == this.getKeyFieldInfo())
            return;
        else if (subFieldInfo == this.getValueFieldInfo())
            this.addValueIndexEntryReturnTypes(types, targetType, this.getKeyFieldInfo().getTypeToken(), valueType);
        else
            throw new RuntimeException();
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <T, K, V> void addValueIndexEntryReturnTypes(List<TypeToken<?>> types,
      TypeToken<T> targetType, TypeToken<K> keyType, TypeToken<V> valueType) {
        types.add(new TypeToken<NavigableMap<V, NavigableSet<MapValueIndexEntry<T, K>>>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<K>() { }, keyType.wrap())
          .where(new TypeParameter<V>() { }, valueType.wrap()));
    }

    @Override
    int getIndexEntryQueryType(TypeToken<?> queryObjectType) {
        return queryObjectType.getRawType().equals(MapValueIndexEntry.class) ? VALUE_INDEX_ENTRY_QUERY : 0;
    }

    @Override
    Method getIndexEntryQueryMethod(int queryType) {
        switch (queryType) {
        case VALUE_INDEX_ENTRY_QUERY:
            return ClassGenerator.QUERY_MAP_FIELD_VALUE_ENTRIES_METHOD;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public NavigableMapConverter<?, ?, ?, ?> getConverter(JTransaction jtx) {
        final Converter<?, ?> keyConverter = this.getKeyFieldInfo().getConverter(jtx);
        final Converter<?, ?> valueConverter = this.getValueFieldInfo().getConverter(jtx);
        return keyConverter != null || valueConverter != null ?
          this.createConverter(
           keyConverter != null ? keyConverter : Converter.identity(),
           valueConverter != null ? valueConverter : Converter.identity()) :
          null;
    }

    // This method exists solely to bind the generic type parameters
    private <K, V, WK, WV> NavigableMapConverter<K, V, WK, WV> createConverter(
      Converter<K, WK> keyConverter, Converter<V, WV> valueConverter) {
        return new NavigableMapConverter<K, V, WK, WV>(keyConverter, valueConverter);
    }

    @Override
    public void copyRecurse(ObjIdSet seen, JTransaction srcTx, JTransaction dstTx,
      ObjId id, int storageId, Deque<Integer> nextFields) {
        final NavigableMap<?, ?> map = srcTx.tx.readMapField(id, this.storageId, false);
        if (storageId == this.getKeyFieldInfo().getStorageId())
            this.copyRecurse(seen, srcTx, dstTx, map.keySet(), nextFields);
        else if (storageId == this.getValueFieldInfo().getStorageId())
            this.copyRecurse(seen, srcTx, dstTx, map.values(), nextFields);
        else
            throw new RuntimeException("internal error");
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final JMapFieldInfo that = (JMapFieldInfo)obj;
        return this.getKeyFieldInfo().equals(that.getKeyFieldInfo()) && this.getValueFieldInfo().equals(that.getValueFieldInfo());
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.getKeyFieldInfo().hashCode() ^ this.getValueFieldInfo().hashCode();
    }
}

