
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.util.ObjIdMap;
import io.permazen.encoding.Encoding;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;
import io.permazen.schema.MapSchemaField;
import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;
import io.permazen.util.ImmutableNavigableMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

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

    @SuppressWarnings("serial")
    MapField(ObjType objType, MapSchemaField field, SimpleField<K> keyField, SimpleField<V> valueField) {
        super(objType, field, new TypeToken<NavigableMap<K, V>>() { }
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

    /**
     * Get the index on this fields's {@value #KEY_FIELD_NAME} sub-field.
     *
     * @return the index on this field's {@value #KEY_FIELD_NAME} sub-field
     * @throws UnknownIndexException if there is no index on the {@value #KEY_FIELD_NAME} sub-field
     */
    @SuppressWarnings("unchecked")
    public MapKeyIndex<K, V> getMapKeyIndex() {
        return (MapKeyIndex<K, V>)this.keyField.getIndex();
    }

    /**
     * Get the index on this fields's {@value #VALUE_FIELD_NAME} sub-field.
     *
     * @return the index on this field's {@value #VALUE_FIELD_NAME} sub-field
     * @throws UnknownIndexException if there is no index on the {@value #VALUE_FIELD_NAME} sub-field
     */
    @SuppressWarnings("unchecked")
    public MapValueIndex<K, V> getMapValueIndex() {
        return (MapValueIndex<K, V>)this.valueField.getIndex();
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
        return (NavigableMap<K, V>)tx.readMapField(id, this.name, false);
    }

    /**
     * Get the key in the underlying key/value store corresponding to this field in the specified object
     * and the specified map key.
     *
     * @param id object ID
     * @param key map key
     * @return the corresponding {@link KVDatabase} key
     * @throws IllegalArgumentException if {@code id} is null or has the wrong object type
     * @see KVTransaction#watchKey KVTransaction.watchKey()
     */
    public ByteData getKey(ObjId id, K key) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");

        // Build key
        final ByteData.Writer writer = ByteData.newWriter();
        writer.write(super.getKey(id));
        this.keyField.encoding.write(writer, key);
        return writer.toByteData();
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
        Preconditions.checkArgument(target != null, "null target");
        return target.caseMapField(this);
    }

// Package Methods

    @Override
    ComplexSubFieldIndex<NavigableMap<K, V>, ?> createSubFieldIndex(
      Schema schema, SimpleSchemaField schemaField, ObjType objType, SimpleField<?> field) {
        Preconditions.checkArgument(schemaField != null, "null schemaField");
        Preconditions.checkArgument(field != null, "null field");
        if (field == this.keyField)
            return new MapKeyIndex<>(schema, schemaField, objType, this);
        if (field == this.valueField)
            return new MapValueIndex<>(schema, schemaField, objType, this);
        throw new IllegalArgumentException("wrong sub-field");
    }

    @Override
    @SuppressWarnings("unchecked")
    <F> Iterable<F> iterateSubField(Transaction tx, ObjId id, SimpleField<F> subField) {
        Preconditions.checkArgument(subField != null, "null subField");
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
        return new ImmutableNavigableMap<>(this.getValueInternal(tx, id));
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
    void buildIndexEntry(ObjId id, SimpleField<?> subField, ByteData content, ByteData value, ByteData.Writer writer) {
        if (subField == this.keyField) {
            writer.write(content);
            id.writeTo(writer);
        } else if (subField == this.valueField) {
            writer.write(value);
            id.writeTo(writer);
            writer.write(content);
        } else
            throw new RuntimeException("internal error");
    }

    @Override
    void unreferenceRemovedTypes(Transaction tx, ObjId id, ReferenceField subField, Set<Integer> removedStorageIds) {
        assert subField == this.keyField || subField == this.valueField;
        for (Iterator<Map.Entry<K, V>> i = this.getValueInternal(tx, id).entrySet().iterator(); i.hasNext(); ) {
            final Map.Entry<K, V> entry = i.next();
            final ObjId ref = subField == this.keyField ? (ObjId)entry.getKey() : (ObjId)entry.getValue();
            if (ref != null && removedStorageIds.contains(ref.getStorageId()))
                i.remove();
        }
    }
}
