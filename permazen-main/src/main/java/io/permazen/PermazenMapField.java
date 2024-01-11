
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.change.MapFieldAdd;
import io.permazen.change.MapFieldClear;
import io.permazen.change.MapFieldRemove;
import io.permazen.change.MapFieldReplace;
import io.permazen.core.MapField;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.schema.MapSchemaField;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Represents a map field in a {@link PermazenClass}.
 */
public class PermazenMapField extends PermazenComplexField {

    final PermazenSimpleField keyField;
    final PermazenSimpleField valueField;

// Constructor

    PermazenMapField(String name, int storageId, io.permazen.annotation.PermazenMapField annotation,
      PermazenSimpleField keyField, PermazenSimpleField valueField, String description, Method getter) {
        super(name, storageId, annotation, description, getter);
        Preconditions.checkArgument(keyField != null, "null keyField");
        Preconditions.checkArgument(valueField != null, "null valueField");
        this.keyField = keyField;
        this.valueField = valueField;
    }

// Public Methods

    @Override
    public io.permazen.annotation.PermazenMapField getDeclaringAnnotation() {
        return (io.permazen.annotation.PermazenMapField)super.getDeclaringAnnotation();
    }

    /**
     * Get the key sub-field.
     *
     * @return this field's key sub-field
     */
    public PermazenSimpleField getKeyField() {
        return this.keyField;
    }

    /**
     * Get the value sub-field.
     *
     * @return this field's value sub-field
     */
    public PermazenSimpleField getValueField() {
        return this.valueField;
    }

    @Override
    public NavigableMap<?, ?> getValue(PermazenObject pobj) {
        Preconditions.checkArgument(pobj != null, "null pobj");
        return pobj.getTransaction().readMapField(pobj.getObjId(), this.name, false);
    }

    @Override
    public <R> R visit(PermazenFieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.casePermazenMapField(this);
    }

    @Override
    public List<PermazenSimpleField> getSubFields() {
        return Arrays.asList(this.keyField, this.valueField);
    }

    @Override
    public MapField<?, ?> getSchemaItem() {
        return (MapField<?, ?>)super.getSchemaItem();
    }

// Package Methods

    @Override
    MapSchemaField createSchemaItem() {
        return new MapSchemaField();
    }

    @Override
    MapSchemaField toSchemaItem() {
        final MapSchemaField schemaField = (MapSchemaField)super.toSchemaItem();
        schemaField.setKeyField(this.keyField.toSchemaItem());
        schemaField.setValueField(this.valueField.toSchemaItem());
        return schemaField;
    }

    @Override
    @SuppressWarnings("unchecked")
    Iterable<ObjId> iterateReferences(Transaction tx, ObjId id, PermazenReferenceField subField) {
        final NavigableMap<?, ?> map = tx.readMapField(id, this.name, false);
        if (subField == this.keyField)
            return (Iterable<ObjId>)map.keySet();
        if (subField == this.valueField)
            return (Iterable<ObjId>)map.values();
        throw new RuntimeException("internal error");
    }

    @Override
    public TypeToken<?> getTypeToken() {
        return this.buildTypeToken(this.keyField.getTypeToken().wrap(), this.valueField.getTypeToken().wrap());
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <K, V> TypeToken<NavigableMap<K, V>> buildTypeToken(TypeToken<K> keyType, TypeToken<V> valueType) {
        return new TypeToken<NavigableMap<K, V>>() { }
          .where(new TypeParameter<K>() { }, keyType)
          .where(new TypeParameter<V>() { }, valueType);
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType) {
        this.addChangeParameterTypes(types, targetType, this.keyField.getTypeToken(), this.valueField.getTypeToken());
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
    public NavigableMapConverter<?, ?, ?, ?> getConverter(PermazenTransaction ptx) {
        Converter<?, ?> keyConverter = this.keyField.getConverter(ptx);
        Converter<?, ?> valueConverter = this.valueField.getConverter(ptx);
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
        return new NavigableMapConverter<>(keyConverter, valueConverter);
    }

// POJO import/export

    @Override
    @SuppressWarnings("unchecked")
    void importPlain(ImportContext context, Object obj, ObjId id) {

        // Get POJO map
        final Map<?, ?> objMap;
        try {
            objMap = (Map<?, ?>)obj.getClass().getMethod(this.getter.getName()).invoke(obj);
        } catch (Exception e) {
            return;
        }
        if (objMap == null)
            return;

        // Get core API map
        final Map<Object, Object> coreMap = (Map<Object, Object>)context.getPermazenTransaction().getTransaction().readMapField(
          id, this.name, true);

        // Copy key/value pairs over
        coreMap.clear();
        for (Map.Entry<?, ?> entry : objMap.entrySet()) {
            coreMap.put(
              this.keyField.importCoreValue(context, entry.getKey()),
              this.valueField.importCoreValue(context, entry.getValue()));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    void exportPlain(ExportContext context, ObjId id, Object obj) {

        // Get POJO map
        final Method objGetter;
        try {
            objGetter = obj.getClass().getMethod(this.getter.getName());
        } catch (Exception e) {
            return;
        }
        Map<Object, Object> objMap;
        try {
            objMap = (Map<Object, Object>)objGetter.invoke(obj);
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to invoke getter method %s for POJO export", objGetter), e);
        }

        // If null, try to create one and identify setter to set it with
        Method objSetter = null;
        if (objMap == null) {
            try {
                objSetter = Util.findPermazenFieldSetterMethod(obj.getClass(), objGetter);
            } catch (IllegalArgumentException e) {
                return;
            }
            final Class<?> mapType = objSetter.getParameterTypes()[0];
            objMap = ConcurrentNavigableMap.class.isAssignableFrom(mapType) ? new ConcurrentSkipListMap<Object, Object>() :
              NavigableMap.class.isAssignableFrom(mapType) ? new TreeMap<Object, Object>() : new HashMap<Object, Object>();
        }

        // Get core API map
        final Map<?, ?> coreMap = context.getPermazenTransaction().getTransaction().readMapField(id, this.name, true);

        // Copy values over
        objMap.clear();
        for (Map.Entry<?, ?> entry : coreMap.entrySet()) {
            objMap.put(
              this.keyField.exportCoreValue(context, entry.getKey()),
              this.valueField.exportCoreValue(context, entry.getValue()));
        }

        // Apply POJO setter if needed
        if (objSetter != null) {
            try {
                objSetter.invoke(obj, objMap);
            } catch (Exception e) {
                throw new RuntimeException(String.format("failed to invoke setter method %s for POJO export", objSetter), e);
            }
        }
    }

// Bytecode generation

    @Override
    Method getFieldReaderMethod() {
        return ClassGenerator.JTRANSACTION_READ_MAP_FIELD_METHOD;
    }
}
