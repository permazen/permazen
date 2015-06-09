
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;

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
     * @param schema schema version
     * @param keyField this field's key sub-field
     * @param valueField this field's value sub-field
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    @SuppressWarnings("serial")
    MapField(String name, int storageId, Schema schema, SimpleField<K> keyField, SimpleField<V> valueField) {
        super(name, storageId, schema, new TypeToken<NavigableMap<K, V>>() { }
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
     *
     * @return map key field
     */
    public SimpleField<K> getKeyField() {
        return this.keyField;
    }

    /**
     * Get the value field.
     *
     * @return map value field
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
        Preconditions.checkArgument(tx != null, "null tx");
        return (NavigableMap<K, V>)tx.readMapField(id, this.storageId, false);
    }

    @Override
    public boolean hasDefaultValue(Transaction tx, ObjId id) {
        return this.getValue(tx, id).isEmpty();
    }

    @Override
    public String toString() {
        return "map field `" + this.name + "' containing key "
          + this.keyField.fieldType + " and value " + this.valueField.fieldType;
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        return target.caseMapField(this);
    }

// Non-public methods

    @Override
    @SuppressWarnings("unchecked")
    <F> Iterable<F> iterateSubField(Transaction tx, ObjId id, SimpleField<F> subField) {
        if (subField == this.keyField)
            return (Iterable<F>)this.getValue(tx, id).keySet();
        if (subField == this.valueField)
            return (Iterable<F>)this.getValue(tx, id).values();
        throw new IllegalArgumentException("unknown sub-field");
    }

    @Override
    NavigableMap<K, V> getValueInternal(Transaction tx, ObjId id) {
        return new JSMap<K, V>(tx, this, id);
    }

    @Override
    NavigableMap<K, V> getValueReadOnlyCopy(Transaction tx, ObjId id) {
        return Maps.unmodifiableNavigableMap(new TreeMap<K, V>(this.getValueInternal(tx, id)));
    }

    @Override
    MapFieldStorageInfo<K, V> toStorageInfo() {
        return new MapFieldStorageInfo<K, V>(this);
    }

    @Override
    void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx) {
        final FieldType<K> keyFieldType = this.keyField.fieldType;
        final NavigableMap<K, V> src = this.getValue(srcTx, srcId);
        final NavigableMap<K, V> dst = this.getValue(dstTx, dstId);
        final Iterator<Map.Entry<K, V>> si = src.entrySet().iterator();
        final Iterator<Map.Entry<K, V>> di = dst.entrySet().iterator();
        if (!si.hasNext()) {
            dst.clear();
            return;
        }
        if (!di.hasNext()) {
            dst.putAll(src);
            return;
        }
        Map.Entry<K, V> s = si.next();
        Map.Entry<K, V> d = di.next();
        while (true) {
            final int diff = keyFieldType.compare(s.getKey(), d.getKey());
            boolean sadvance = true;
            boolean dadvance = true;
            if (diff < 0) {
                dst.put(s.getKey(), s.getValue());
                dadvance = false;
            } else if (diff > 0) {
                di.remove();
                sadvance = false;
            } else
                d.setValue(s.getValue());
            if (sadvance) {
                if (!si.hasNext()) {
                    dst.tailMap(s.getKey(), false).clear();
                    return;
                }
                s = si.next();
            }
            if (dadvance) {
                if (!di.hasNext()) {
                    dst.putAll(src.tailMap(s.getKey(), true));
                    return;
                }
                d = di.next();
            }
        }
    }

    @Override
    void buildIndexEntry(ObjId id, SimpleField<?> subField, ByteReader reader, byte[] value, ByteWriter writer) {
        if (subField == this.keyField) {
            writer.write(reader);
            id.writeTo(writer);
        } else if (subField == this.valueField) {
            writer.write(value);
            id.writeTo(writer);
            writer.write(reader);
        } else
            throw new RuntimeException("internal error");
    }

    @Override
    void unreferenceRemovedObjectTypes(Transaction tx, ObjId id, ReferenceField subField, SortedSet<Integer> removedStorageIds) {
        assert subField == this.keyField || subField == this.valueField;
        for (Iterator<Map.Entry<K, V>> i = this.getValueInternal(tx, id).entrySet().iterator(); i.hasNext(); ) {
            final Map.Entry<K, V> entry = i.next();
            final ObjId ref = subField == this.keyField ? (ObjId)entry.getKey() : (ObjId)entry.getValue();
            if (ref != null && removedStorageIds.contains(ref.getStorageId()))
                i.remove();
        }
    }
}

