
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * Implements the {@link NavigableMap} view of a {@link MapField}.
 */
class JSMap<K, V> extends FieldTypeMap<K, V> {

    private final ObjId id;
    private final MapField<K, V> field;

    /**
     * Primary constructor.
     */
    JSMap(Transaction tx, MapField<K, V> field, ObjId id) {
        super(tx, field.keyField.fieldType, false, field.buildKey(id));
        this.id = id;
        this.field = field;
    }

    /**
     * Internal constructor.
     */
    JSMap(Transaction tx, MapField<K, V> field, ObjId id, boolean reversed, byte[] prefix, KeyRanges keyRanges, Bounds<K> bounds) {
        super(tx, field.keyField.fieldType, false, reversed, prefix, keyRanges, bounds);
        this.id = id;
        this.field = field;
    }

    @Override
    public V put(final K keyObj, final V valueObj) {
        final byte[] key;
        final byte[] value;
        try {
            key = this.encodeKey(keyObj);
            value = this.encodeValue(valueObj);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("can't add invalid key/value pair to " + this.field + ": " + e.getMessage(), e);
        }
        if (!this.isVisible(key))
            throw new IllegalArgumentException("key out of range");
        return this.tx.mutateAndNotify(this.id, new Transaction.Mutation<V>() {
            @Override
            public V mutate() {
                return JSMap.this.doPut(keyObj, valueObj, key, value);
            }
        });
    }

    private V doPut(final K keyObj, final V newValueObj, byte[] key, byte[] newValue) {

        // Get old value, if any
        final byte[] oldValue = this.tx.kvt.get(key);
        final V oldValueObj;
        if (oldValue != null) {

            // Decode old value
            oldValueObj = this.decodeValue(new KVPair(key, oldValue));

            // Optimize if no change
            if (Arrays.equals(newValue, oldValue))
                return oldValueObj;

            // Remove index entries for old value
            if (this.field.keyField.indexed)
                this.field.removeIndexEntry(this.tx, this.id, this.field.keyField, key, oldValue);
            if (this.field.valueField.indexed)
                this.field.removeIndexEntry(this.tx, this.id, this.field.valueField, key, oldValue);
        } else
            oldValueObj = null;

        // Put new value
        this.tx.kvt.put(key, newValue);

        // Add index entries for new value
        if (this.field.keyField.indexed)
            this.field.addIndexEntry(this.tx, this.id, this.field.keyField, key, newValue);
        if (this.field.valueField.indexed)
            this.field.addIndexEntry(this.tx, this.id, this.field.valueField, key, newValue);

        // Notify field monitors
        this.tx.addFieldChangeNotification(new MapFieldChangeNotifier() {
            @Override
            void notify(Transaction tx, MapFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
                if (oldValue == null)
                    listener.onMapFieldAdd(tx, this.getId(), JSMap.this.field, path, referrers, keyObj, newValueObj);
                else {
                    listener.onMapFieldReplace(tx, this.getId(),
                      JSMap.this.field, path, referrers, keyObj, oldValueObj, newValueObj);
                }
            }
        });

        // Done
        return oldValueObj;
    }

    @Override
    public V remove(final Object keyObj) {
        byte[] key;
        try {
            key = this.encodeKey(keyObj);
        } catch (IllegalArgumentException e) {
            return null;
        }
        if (!this.isVisible(key))
            return null;
        final byte[] key2 = key;
        final K keyObj2 = this.keyFieldType.validate(keyObj);
        return this.tx.mutateAndNotify(this.id, new Transaction.Mutation<V>() {
            @Override
            public V mutate() {
                return JSMap.this.doRemove(keyObj2, key2);
            }
        });
    }

    private V doRemove(final K keyObj, byte[] key) {

        // Get old value, if any
        final byte[] oldValue = this.tx.kvt.get(key);
        if (oldValue == null)
            return null;
        final V valueObj = this.decodeValue(new KVPair(key, oldValue));

        // Remove entry
        this.tx.kvt.remove(key);

        // Remove index entries for old value
        if (this.field.keyField.indexed)
            this.field.removeIndexEntry(this.tx, this.id, this.field.keyField, key, oldValue);
        if (this.field.valueField.indexed)
            this.field.removeIndexEntry(this.tx, this.id, this.field.valueField, key, oldValue);

        // Notify field monitors
        this.tx.addFieldChangeNotification(new MapFieldChangeNotifier() {
            @Override
            void notify(Transaction tx, MapFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
                listener.onMapFieldRemove(tx, this.getId(), JSMap.this.field, path, referrers, keyObj, valueObj);
            }
        });

        // Done
        return valueObj;
    }

    @Override
    public void clear() {
        this.tx.mutateAndNotify(this.id, new Transaction.Mutation<Void>() {
            @Override
            public Void mutate() {
                JSMap.this.doClear();
                return null;
            }
        });
    }

    private void doClear() {

        // Optimize if already empty
        if (this.isEmpty())
            return;

        // If range is restricted and there are field monitors, use individual deletions so we get individual notifications
        if (!this.bounds.equals(new Bounds<K>()) && this.tx.hasFieldMonitor(this.id, this.field)) {
            for (Iterator<Map.Entry<K, V>> i = this.entrySet().iterator(); i.hasNext(); ) {
                i.next();
                i.remove();
            }
            return;
        }

        // Get key range
        final List<KeyRange> keyRangeList = this.keyRanges.getKeyRanges();
        if (keyRangeList.size() > 1)
            throw new UnsupportedOperationException("clear() not supported with complex key range restriction: " + this.keyRanges);
        final KeyRange keyRange = keyRangeList.get(0);
        final byte[] rangeMinKey = keyRange.getMin();
        final byte[] rangeMaxKey = keyRange.getMax();

        // Delete index entries
        if (this.field.keyField.indexed)
            this.field.removeIndexEntries(this.tx, this.id, this.field.keyField, rangeMinKey, rangeMaxKey);
        if (this.field.valueField.indexed)
            this.field.removeIndexEntries(this.tx, this.id, this.field.valueField, rangeMinKey, rangeMaxKey);

        // Delete content
        this.field.deleteContent(this.tx, rangeMinKey, rangeMaxKey);

        // Notify field monitors
        this.tx.addFieldChangeNotification(new MapFieldChangeNotifier() {
            @Override
            void notify(Transaction tx, MapFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
                listener.onMapFieldClear(tx, this.getId(), JSMap.this.field, path, referrers);
            }
        });
    }

    @Override
    protected NavigableMap<K, V> createSubMap(boolean newReversed, KeyRanges newKeyRanges, Bounds<K> newBounds) {
        return new JSMap<K, V>(this.tx, this.field, this.id, newReversed, this.prefix, newKeyRanges, newBounds);
    }

    private byte[] encodeKey(Object obj) {
        final ByteWriter writer = new ByteWriter();
        this.encodeKey(writer, obj);
        return writer.getBytes();
    }

    private byte[] encodeValue(Object obj) {
        final ByteWriter writer = new ByteWriter();
        this.field.valueField.fieldType.validateAndWrite(writer, obj);
        return writer.getBytes();
    }

    @Override
    protected V decodeValue(KVPair pair) {
        return this.field.valueField.fieldType.read(new ByteReader(pair.getValue()));
    }

// MapFieldChangeNotifier

    private abstract class MapFieldChangeNotifier implements FieldChangeNotifier {

        @Override
        public int getStorageId() {
            return JSMap.this.field.storageId;
        }

        @Override
        public ObjId getId() {
            return JSMap.this.id;
        }

        @Override
        public void notify(Transaction tx, Object listener, int[] path, NavigableSet<ObjId> referrers) {
            this.notify(tx, (MapFieldChangeListener)listener, path, referrers);
        }

        abstract void notify(Transaction tx, MapFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers);
    }
}

