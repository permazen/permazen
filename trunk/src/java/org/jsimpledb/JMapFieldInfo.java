
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.util.List;
import java.util.NavigableMap;

import org.jsimpledb.change.MapFieldAdd;
import org.jsimpledb.change.MapFieldClear;
import org.jsimpledb.change.MapFieldRemove;
import org.jsimpledb.change.MapFieldReplace;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjIdSet;
import org.jsimpledb.core.Transaction;

class JMapFieldInfo extends JComplexFieldInfo {

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
    public TypeToken<?> getTypeToken(Class<?> context) {
        return this.buildTypeToken(
          this.getKeyFieldInfo().getTypeToken(context).wrap(),
          this.getValueFieldInfo().getTypeToken(context).wrap());
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
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType) {
        this.addChangeParameterTypes(types, targetType,
          this.getKeyFieldInfo().getTypeToken(targetType),
          this.getValueFieldInfo().getTypeToken(targetType));
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <T, K, V> void addChangeParameterTypes(List<TypeToken<?>> types,
      Class<T> targetType, TypeToken<K> keyType, TypeToken<V> valueType) {
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
      ObjId id, int storageId, int fieldIndex, int[] fieldIds) {
        final NavigableMap<?, ?> map = srcTx.tx.readMapField(id, this.storageId, false);
        if (storageId == this.getKeyFieldInfo().getStorageId())
            this.copyRecurse(seen, srcTx, dstTx, map.keySet(), fieldIndex, fieldIds);
        else if (storageId == this.getValueFieldInfo().getStorageId())
            this.copyRecurse(seen, srcTx, dstTx, map.values(), fieldIndex, fieldIds);
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

