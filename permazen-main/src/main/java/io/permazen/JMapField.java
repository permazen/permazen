
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
 * Represents a map field in a {@link JClass}.
 */
public class JMapField extends JComplexField {

    final JSimpleField keyField;
    final JSimpleField valueField;

    JMapField(Permazen jdb, String name, int storageId, io.permazen.annotation.JMapField annotation,
      JSimpleField keyField, JSimpleField valueField, String description, Method getter) {
        super(jdb, name, storageId, annotation, description, getter);
        Preconditions.checkArgument(keyField != null, "null keyField");
        Preconditions.checkArgument(valueField != null, "null valueField");
        this.keyField = keyField;
        this.valueField = valueField;
    }

    @Override
    public io.permazen.annotation.JMapField getDeclaringAnnotation() {
        return (io.permazen.annotation.JMapField)super.getDeclaringAnnotation();
    }

    /**
     * Get the key sub-field.
     *
     * @return this field's key sub-field
     */
    public JSimpleField getKeyField() {
        return this.keyField;
    }

    /**
     * Get the value sub-field.
     *
     * @return this field's value sub-field
     */
    public JSimpleField getValueField() {
        return this.valueField;
    }

    @Override
    public NavigableMap<?, ?> getValue(JObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        return jobj.getTransaction().readMapField(jobj.getObjId(), this.storageId, false);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJMapField(this);
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
        throw new IllegalArgumentException("unknown sub-field \"" + name
          + "\" (did you mean \"" + MapField.KEY_FIELD_NAME + "\" or \"" + MapField.VALUE_FIELD_NAME + "\" instead?)");
    }

    @Override
    boolean isSameAs(JField that0) {
        if (!super.isSameAs(that0))
            return false;
        final JMapField that = (JMapField)that0;
        if (!this.keyField.isSameAs(that.keyField))
            return false;
        if (!this.valueField.isSameAs(that.valueField))
            return false;
        return true;
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
    MapSchemaField toSchemaItem(Permazen jdb) {
        final MapSchemaField schemaField = new MapSchemaField();
        super.initialize(jdb, schemaField);
        schemaField.setKeyField(this.keyField.toSchemaItem(jdb));
        schemaField.setValueField(this.valueField.toSchemaItem(jdb));
        return schemaField;
    }

    @Override
    ComplexSubFieldIndexInfo toIndexInfo(JSimpleField subField) {
        if (subField == this.keyField)
            return new MapKeyIndexInfo(this);
        if (subField == this.valueField)
            return new MapValueIndexInfo(this);
        throw new IllegalArgumentException("unknown sub-field");
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
    public NavigableMapConverter<?, ?, ?, ?> getConverter(JTransaction jtx) {
        Converter<?, ?> keyConverter = this.keyField.getConverter(jtx);
        Converter<?, ?> valueConverter = this.valueField.getConverter(jtx);
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
        final Map<Object, Object> coreMap = (Map<Object, Object>)context.getTransaction().getTransaction().readMapField(
          id, this.storageId, true);

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
            throw new RuntimeException("failed to invoke getter method " + objGetter + " for POJO export", e);
        }

        // If null, try to create one and identify setter to set it with
        Method objSetter = null;
        if (objMap == null) {
            try {
                objSetter = Util.findJFieldSetterMethod(obj.getClass(), objGetter);
            } catch (IllegalArgumentException e) {
                return;
            }
            final Class<?> mapType = objSetter.getParameterTypes()[0];
            objMap = ConcurrentNavigableMap.class.isAssignableFrom(mapType) ? new ConcurrentSkipListMap<Object, Object>() :
              NavigableMap.class.isAssignableFrom(mapType) ? new TreeMap<Object, Object>() : new HashMap<Object, Object>();
        }

        // Get core API map
        final Map<?, ?> coreMap = context.getTransaction().getTransaction().readMapField(id, this.storageId, true);

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
                throw new RuntimeException("failed to invoke setter method " + objSetter + " for POJO export", e);
            }
        }
    }

// Bytecode generation

    @Override
    Method getFieldReaderMethod() {
        return ClassGenerator.JTRANSACTION_READ_MAP_FIELD_METHOD;
    }
}

