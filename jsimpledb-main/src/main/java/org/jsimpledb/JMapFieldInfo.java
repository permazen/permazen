
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import org.jsimpledb.change.MapFieldAdd;
import org.jsimpledb.change.MapFieldClear;
import org.jsimpledb.change.MapFieldRemove;
import org.jsimpledb.change.MapFieldReplace;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
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
    public Set<TypeToken<?>> getTypeTokens(Class<?> context) {
        final HashSet<TypeToken<?>> typeTokens = new HashSet<>();
        for (TypeToken<?> keyTypeToken : this.getKeyFieldInfo().getTypeTokens(context)) {
            for (TypeToken<?> valueTypeToken : this.getValueFieldInfo().getTypeTokens(context))
                typeTokens.add(this.buildTypeToken(keyTypeToken.wrap(), valueTypeToken.wrap()));
        }
        return typeTokens;
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
        for (TypeToken<?> keyTypeToken : this.getKeyFieldInfo().getTypeTokens(targetType)) {
            for (TypeToken<?> valueTypeToken : this.getValueFieldInfo().getTypeTokens(targetType))
                this.addChangeParameterTypes(types, targetType, keyTypeToken, valueTypeToken);
        }
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
        Converter<?, ?> keyConverter = this.getKeyFieldInfo().getConverter(jtx);
        Converter<?, ?> valueConverter = this.getValueFieldInfo().getConverter(jtx);
        if (keyConverter == null && valueConverter == null)
            return null;
        if (keyConverter == null)
           keyConverter = Converter.<Object>identity();
        if (valueConverter == null)
           valueConverter = Converter.<Object>identity();
        return this.createConverter(keyConverter, valueConverter);
    }

    // This method exists solely to bind the generic type parameters
    private <K, V, WK, WV> NavigableMapConverter<K, V, WK, WV> createConverter(
      Converter<K, WK> keyConverter, Converter<V, WV> valueConverter) {
        return new NavigableMapConverter<K, V, WK, WV>(keyConverter, valueConverter);
    }

    @Override
    public void copyRecurse(CopyState copyState, JTransaction srcTx, JTransaction dstTx,
      ObjId id, int storageId, int fieldIndex, int[] fields) {
        final NavigableMap<?, ?> map = srcTx.tx.readMapField(id, this.storageId, false);
        if (storageId == this.getKeyFieldInfo().getStorageId())
            this.copyRecurse(copyState, srcTx, dstTx, map.keySet(), fieldIndex, fields);
        else if (storageId == this.getValueFieldInfo().getStorageId())
            this.copyRecurse(copyState, srcTx, dstTx, map.values(), fieldIndex, fields);
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

