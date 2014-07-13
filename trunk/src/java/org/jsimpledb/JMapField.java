
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
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.jsimpledb.change.MapFieldAdd;
import org.jsimpledb.change.MapFieldClear;
import org.jsimpledb.change.MapFieldRemove;
import org.jsimpledb.change.MapFieldReplace;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.MapSchemaField;
import org.objectweb.asm.ClassWriter;

/**
 * Represents a map field in a {@link JClass}.
 */
public class JMapField extends JComplexField {

    private static final int KEY_INDEX_ENTRY_QUERY = 1;
    private static final int VALUE_INDEX_ENTRY_QUERY = 2;

    final JSimpleField keyField;
    final JSimpleField valueField;

    JMapField(String name, int storageId, JSimpleField keyField, JSimpleField valueField, String description, Method getter) {
        super(name, storageId, description, getter);
        if (keyField == null)
            throw new IllegalArgumentException("null keyField");
        if (valueField == null)
            throw new IllegalArgumentException("null valueField");
        this.keyField = keyField;
        this.valueField = valueField;
    }

    /**
     * Get the key sub-field.
     */
    public JSimpleField getKeyField() {
        return this.keyField;
    }

    /**
     * Get the value sub-field.
     */
    public JSimpleField getValueField() {
        return this.valueField;
    }

    @Override
    public NavigableMap<?, ?> getValue(JTransaction jtx, ObjId id) {
        if (jtx == null)
            throw new IllegalArgumentException("null jtx");
        return jtx.readMapField(id, this.storageId, false);
    }

    @Override
    public List<JSimpleField> getSubFields() {
        return Arrays.asList(this.keyField, this.valueField);
    }

    @Override
    public JSimpleField getSubField(String name) {
        if (MapField.KEY_FIELD_NAME.equals(name))
            return this.keyField;
        if (MapField.VALUE_FIELD_NAME.equals(name))
            return this.valueField;
        throw new IllegalArgumentException("unknown sub-field `" + name
          + "' (did you mean `" + MapField.KEY_FIELD_NAME + "' or `" + MapField.VALUE_FIELD_NAME + "' instead?)");
    }

    @Override
    String getSubFieldName(JSimpleField subField) {
        if (subField == this.keyField)
            return MapField.KEY_FIELD_NAME;
        if (subField == this.valueField)
            return MapField.VALUE_FIELD_NAME;
        throw new IllegalArgumentException("unknown sub-field");
    }

    @Override
    MapSchemaField toSchemaItem() {
        final MapSchemaField schemaField = new MapSchemaField();
        super.initialize(schemaField);
        schemaField.setKeyField(this.keyField.toSchemaItem());
        schemaField.setValueField(this.valueField.toSchemaItem());
        return schemaField;
    }

    @Override
    void outputMethods(ClassGenerator<?> generator, ClassWriter cw) {
        this.outputReadMethod(generator, cw, ClassGenerator.READ_MAP_FIELD_METHOD);
    }

    @Override
    void registerChangeListener(Transaction tx, int[] path, AllChangesListener listener) {
        tx.addMapFieldChangeListener(this.storageId, path, listener);
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, TypeToken<T> targetType) {
        this.addChangeParameterTypes(types, targetType, this.keyField.typeToken, this.valueField.typeToken);
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
    <T> void addIndexEntryReturnTypes(List<TypeToken<?>> types, TypeToken<T> targetType, JSimpleField subField) {
        if (subField == this.keyField)
            this.addKeyIndexEntryReturnTypes(types, targetType, this.keyField.typeToken, this.valueField.typeToken);
        else if (subField == this.valueField)
            this.addValueIndexEntryReturnTypes(types, targetType, this.keyField.typeToken, this.valueField.typeToken);
        else
            throw new RuntimeException();
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <T, K, V> void addKeyIndexEntryReturnTypes(List<TypeToken<?>> types,
      TypeToken<T> targetType, TypeToken<K> keyType, TypeToken<V> valueType) {
        types.add(new TypeToken<NavigableMap<K, NavigableSet<MapKeyIndexEntry<T, V>>>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<K>() { }, keyType.wrap())
          .where(new TypeParameter<V>() { }, valueType.wrap()));
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
        return queryObjectType.getRawType().equals(MapKeyIndexEntry.class) ? KEY_INDEX_ENTRY_QUERY :
          queryObjectType.getRawType().equals(MapValueIndexEntry.class) ? VALUE_INDEX_ENTRY_QUERY : 0;
    }

    @Override
    Method getIndexEntryQueryMethod(int queryType) {
        switch (queryType) {
        case KEY_INDEX_ENTRY_QUERY:
            return ClassGenerator.QUERY_MAP_FIELD_KEY_ENTRIES_METHOD;
        case VALUE_INDEX_ENTRY_QUERY:
            return ClassGenerator.QUERY_MAP_FIELD_VALUE_ENTRIES_METHOD;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    NavigableMapConverter<?, ?, ?, ?> getConverter(JTransaction jtx) {
        final Converter<?, ?> keyConverter = this.keyField.getConverter(jtx);
        final Converter<?, ?> valueConverter = this.valueField.getConverter(jtx);
        if (keyConverter == null && valueConverter == null)
            return null;
        return this.createConverter(keyConverter != null ? keyConverter : Converter.identity(),
          valueConverter != null ? valueConverter : Converter.identity());
    }

    // This method exists solely to bind the generic type parameters
    private <K, V, WK, WV> NavigableMapConverter<K, V, WK, WV> createConverter(
      Converter<K, WK> keyConverter, Converter<V, WV> valueConverter) {
        return new NavigableMapConverter<K, V, WK, WV>(keyConverter, valueConverter);
    }

    @Override
    void copyRecurse(Set<ObjId> seen, JTransaction srcTx, JTransaction dstTx,
      ObjId id, JReferenceField subField, Deque<JReferenceField> nextFields) {
        final NavigableMap<?, ?> map = srcTx.tx.readMapField(id, this.storageId, false);
        if (subField == this.keyField)
            this.copyRecurse(seen, srcTx, dstTx, map.keySet(), nextFields);
        else if (subField == this.valueField)
            this.copyRecurse(seen, srcTx, dstTx, map.values(), nextFields);
        else
            throw new RuntimeException();
    }
}

