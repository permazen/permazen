
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.MapField;
import org.jsimpledb.Transaction;
import org.jsimpledb.jlayer.change.FieldChange;
import org.jsimpledb.jlayer.change.MapFieldAdd;
import org.jsimpledb.jlayer.change.MapFieldChange;
import org.jsimpledb.jlayer.change.MapFieldClear;
import org.jsimpledb.jlayer.change.MapFieldRemove;
import org.jsimpledb.jlayer.change.MapFieldReplace;
import org.jsimpledb.schema.MapSchemaField;
import org.jsimpledb.util.ConvertedNavigableMap;
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
        types.add(new TypeToken<FieldChange<T>>() { }
          .where(new TypeParameter<T>() { }, targetType));
        types.add(new TypeToken<MapFieldChange<T, K, V>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<K>() { }, keyType.wrap())
          .where(new TypeParameter<V>() { }, valueType.wrap()));
        types.add(new TypeToken<MapFieldAdd<T, K, V>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<K>() { }, keyType.wrap())
          .where(new TypeParameter<V>() { }, valueType.wrap()));
        types.add(new TypeToken<MapFieldClear<T, K, V>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<K>() { }, keyType.wrap())
          .where(new TypeParameter<V>() { }, valueType.wrap()));
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
    NavigableMap<?, ?> convert(ReferenceConverter converter, Object value) {
        return JMapField.convert(converter, value,
          this.keyField instanceof JReferenceField, this.valueField instanceof JReferenceField);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static NavigableMap<?, ?> convert(ReferenceConverter converter, Object value, boolean refKey, boolean refValue) {
        NavigableMap<?, ?> map = (NavigableMap<?, ?>)value;
        if (refKey || refValue) {
            map = new ConvertedNavigableMap(map,
              refKey ? converter : Converter.identity(),
              refValue ? converter : Converter.identity());
        }
        return map;
    }
}

