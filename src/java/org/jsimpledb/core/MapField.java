
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * Map field.
 *
 * @param <K> Map key type
 * @param <V> Map value type
 */
public class MapField<K, V> extends ComplexField<NavigableMap<K, V>> {

    public static final String KEY_FIELD_NAME = "key";
    public static final String VALUE_FIELD_NAME = "value";

    final SimpleField<K> keyField;
    final SimpleField<V> valueField;

    /**
     * Constructor.
     *
     * @param objType the object type that contains this field
     * @param name the name of the field
     * @param storageId field content storage ID
     * @param version schema version
     * @param keyField this field's key sub-field
     * @param valueField this field's value sub-field
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    @SuppressWarnings("serial")
    MapField(String name, int storageId, SchemaVersion version, SimpleField<K> keyField, SimpleField<V> valueField) {
        super(name, storageId, version, new TypeToken<NavigableMap<K, V>>() { }
          .where(new TypeParameter<K>() { }, keyField.typeToken.wrap())
          .where(new TypeParameter<V>() { }, valueField.typeToken.wrap()));
        this.keyField = keyField;
        this.valueField = valueField;
        assert this.keyField.parent == null;
        assert this.valueField.parent == null;
        this.keyField.parent = this;
        this.valueField.parent = this;
    }

// Public methods

    /**
     * Get the key field.
     */
    public SimpleField<K> getKeyField() {
        return this.keyField;
    }

    /**
     * Get the value field.
     */
    public SimpleField<V> getValueField() {
        return this.valueField;
    }

    @Override
    public List<SimpleField<?>> getSubFields() {
        final ArrayList<SimpleField<?>> list = new ArrayList<>(2);
        list.add(this.keyField);
        list.add(this.valueField);
        return list;
    }

    @Override
    @SuppressWarnings("unchecked")
    public NavigableMap<K, V> getValue(Transaction tx, ObjId id) {
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        return (NavigableMap<K, V>)tx.readMapField(id, this.storageId, false);
    }

    @Override
    public boolean hasDefaultValue(Transaction tx, ObjId id) {
        return this.getValue(tx, id).isEmpty();
    }

    @Override
    public String toString() {
        return "map field `" + this.name + "' of " + this.keyField.fieldType + ", " + this.valueField.fieldType;
    }

// Non-public methods

    @Override
    NavigableMap<K, V> getValueInternal(Transaction tx, ObjId id) {
        return new JSMap<K, V>(tx, this, id);
    }

    @Override
    MapFieldStorageInfo toStorageInfo() {
        return new MapFieldStorageInfo(this);
    }

    @Override
    boolean hasComplexIndex(SimpleField<?> subField) {
        return true;        // index value = object ID + (key or value)
    }

    @Override
    void buildIndexEntry(ObjId id, SimpleField<?> subField, ByteReader reader, byte[] value, ByteWriter writer) {
        if (subField == this.keyField) {
            writer.write(reader);
            id.writeTo(writer);
            writer.write(value);
        } else if (subField == this.valueField) {
            writer.write(value);
            id.writeTo(writer);
            writer.write(reader);
        } else
            throw new RuntimeException("internal error");
    }
}

