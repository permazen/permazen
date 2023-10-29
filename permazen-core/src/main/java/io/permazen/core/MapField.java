
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.encoding.Encoding;
import io.permazen.core.util.ObjIdMap;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.CloseableIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;

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
        return "map field \"" + this.name + "\" containing key "
          + this.keyField.encoding + " and value " + this.valueField.encoding;
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
        return new JSMap<>(tx, this, id);
    }

    @Override
    NavigableMap<K, V> getValueReadOnlyCopy(Transaction tx, ObjId id) {
        return Collections.unmodifiableNavigableMap(new TreeMap<K, V>(this.getValueInternal(tx, id)));
    }

    @Override
    ComplexSubFieldStorageInfo<?, ?> toStorageInfo(SimpleField<?> subField) {
        if (subField == this.keyField)
            return new MapKeyStorageInfo<K>(this);
        if (subField == this.valueField)
            return new MapValueStorageInfo<K, V>(this);
        throw new IllegalArgumentException("unknown sub-field");
    }

    @Override
    void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx, ObjIdMap<ObjId> objectIdMap) {
        final Encoding<K> keyEncoding = this.keyField.encoding;
        final NavigableMap<K, V> src = this.getValue(srcTx, srcId);
        final NavigableMap<K, V> dst = this.getValue(dstTx, dstId);
        try (CloseableIterator<Map.Entry<K, V>> si = CloseableIterator.wrap(src.entrySet().iterator());
             CloseableIterator<Map.Entry<K, V>> di = CloseableIterator.wrap(dst.entrySet().iterator())) {

            // Check for empty
            if (!si.hasNext()) {
                dst.clear();
                return;
            }

            // If we're not remapping anything, walk forward through both maps and synchronize dst to src
            if (objectIdMap == null || objectIdMap.isEmpty()
              || (!this.keyField.remapsObjectId() && !this.valueField.remapsObjectId())) {
                if (!di.hasNext()) {
                    dst.putAll(src);
                    return;
                }
                Map.Entry<K, V> s = si.next();
                Map.Entry<K, V> d = di.next();
                while (true) {
                    final int diff = keyEncoding.compare(s.getKey(), d.getKey());
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
            } else {
                dst.clear();
                while (si.hasNext()) {
                    final Map.Entry<K, V> entry = si.next();
                    final K destKey = this.keyField.remapObjectId(objectIdMap, entry.getKey());
                    final V destValue = this.valueField.remapObjectId(objectIdMap, entry.getValue());
                    dst.put(destKey, destValue);
                }
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
    void unreferenceRemovedTypes(Transaction tx, ObjId id, ReferenceField subField, SortedSet<Integer> removedStorageIds) {
        assert subField == this.keyField || subField == this.valueField;
        for (Iterator<Map.Entry<K, V>> i = this.getValueInternal(tx, id).entrySet().iterator(); i.hasNext(); ) {
            final Map.Entry<K, V> entry = i.next();
            final ObjId ref = subField == this.keyField ? (ObjId)entry.getKey() : (ObjId)entry.getValue();
            if (ref != null && removedStorageIds.contains(ref.getStorageId()))
                i.remove();
        }
    }

    @Override
    boolean isUpgradeCompatible(Field<?> field) {
        if (field.getClass() != this.getClass())
            return false;
        final MapField<?, ?> that = (MapField<?, ?>)field;
        return this.keyField.isUpgradeCompatible(that.keyField)
          && this.valueField.isUpgradeCompatible(that.valueField);
    }
}
