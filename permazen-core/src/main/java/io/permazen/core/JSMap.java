
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.util.Bounds;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Implements the {@link NavigableMap} view of a {@link MapField}.
 */
class JSMap<K, V> extends FieldTypeMap<K, V> {

    private final Transaction tx;
    private final ObjId id;
    private final MapField<K, V> field;

    /**
     * Primary constructor.
     */
    JSMap(Transaction tx, MapField<K, V> field, ObjId id) {
        super(tx.kvt, field.keyField.fieldType, false, field.buildKey(id));
        this.tx = tx;
        this.id = id;
        this.field = field;
    }

    /**
     * Internal constructor.
     */
    private JSMap(Transaction tx, MapField<K, V> field, ObjId id,
      boolean reversed, KeyRange keyRange, KeyFilter keyFilter, Bounds<K> bounds) {
        super(tx.kvt, field.keyField.fieldType, false, reversed, field.buildKey(id), keyRange, keyFilter, bounds);
        Preconditions.checkArgument(keyRange != null, "null keyRange");
        this.tx = tx;
        this.id = id;
        this.field = field;
    }

    @Override
    public V put(final K keyObj, final V valueObj) {
        final byte[] key;
        final byte[] value;
        try {
            key = this.encodeVisibleKey(keyObj, true);
            value = this.encodeValue(valueObj);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("can't add invalid key/value pair to " + this.field + ": " + e.getMessage(), e);
        }
        return this.tx.mutateAndNotify(this.id, () -> this.doPut(keyObj, valueObj, key, value));
    }

    private V doPut(final K keyObj, final V newValueObj, byte[] key, byte[] newValue) {

        // Check for deleted assignment of key and/or value
        if (this.field.keyField instanceof ReferenceField)
            this.tx.checkDeletedAssignment(this.id, (ReferenceField)this.field.keyField, (ObjId)keyObj);
        if (this.field.valueField instanceof ReferenceField)
            this.tx.checkDeletedAssignment(this.id, (ReferenceField)this.field.valueField, (ObjId)newValueObj);

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
        if (!this.tx.disableListenerNotifications) {
            this.tx.addFieldChangeNotification(new MapFieldChangeNotifier() {
                @Override
                public void notify(Transaction tx, MapFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
                    if (oldValue == null)
                        listener.onMapFieldAdd(tx, this.id, JSMap.this.field, path, referrers, keyObj, newValueObj);
                    else {
                        listener.onMapFieldReplace(tx, this.id,
                          JSMap.this.field, path, referrers, keyObj, oldValueObj, newValueObj);
                    }
                }
            });
        }

        // Done
        return oldValueObj;
    }

    @Override
    public V remove(Object keyObj) {
        final byte[] key = this.encodeVisibleKey(keyObj, false);
        if (key == null)
            return null;
        final K canonicalKey = this.keyFieldType.validate(keyObj);
        return this.tx.mutateAndNotify(this.id, () -> this.doRemove(canonicalKey, key));
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
        if (!this.tx.disableListenerNotifications) {
            this.tx.addFieldChangeNotification(new MapFieldChangeNotifier() {
                @Override
                public void notify(Transaction tx, MapFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
                    listener.onMapFieldRemove(tx, this.id, JSMap.this.field, path, referrers, keyObj, valueObj);
                }
            });
        }

        // Done
        return valueObj;
    }

    @Override
    public void clear() {
        if (this.keyFilter != null)
            throw new UnsupportedOperationException("clear() not supported when KeyFilter configured");
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
        if (!this.bounds.equals(new Bounds<K>()) && this.tx.hasFieldMonitor(this.id, this.field.storageId)) {
            for (Iterator<Map.Entry<K, V>> i = this.entrySet().iterator(); i.hasNext(); ) {
                i.next();
                i.remove();
            }
            return;
        }

        // Get key range
        final byte[] rangeMinKey = this.keyRange.getMin();
        final byte[] rangeMaxKey = this.keyRange.getMax();

        // Delete index entries
        if (this.field.keyField.indexed)
            this.field.removeIndexEntries(this.tx, this.id, this.field.keyField, rangeMinKey, rangeMaxKey);
        if (this.field.valueField.indexed)
            this.field.removeIndexEntries(this.tx, this.id, this.field.valueField, rangeMinKey, rangeMaxKey);

        // Delete content
        this.field.deleteContent(this.tx, rangeMinKey, rangeMaxKey);

        // Notify field monitors
        if (!this.tx.disableListenerNotifications) {
            this.tx.addFieldChangeNotification(new MapFieldChangeNotifier() {
                @Override
                public void notify(Transaction tx, MapFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
                    listener.onMapFieldClear(tx, this.id, JSMap.this.field, path, referrers);
                }
            });
        }
    }

    @Override
    protected NavigableMap<K, V> createSubMap(boolean newReversed,
      KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<K> newBounds) {
        return new JSMap<>(this.tx, this.field, this.id, newReversed, newKeyRange, newKeyFilter, newBounds);
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

    private abstract class MapFieldChangeNotifier extends FieldChangeNotifier<MapFieldChangeListener> {

        MapFieldChangeNotifier() {
            super(MapFieldChangeListener.class, JSMap.this.field.storageId, JSMap.this.id);
        }
    }
}

