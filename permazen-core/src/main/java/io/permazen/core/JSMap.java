
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.util.Bounds;
import io.permazen.util.ByteData;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Implements the {@link NavigableMap} view of a {@link MapField}.
 */
class JSMap<K, V> extends EncodingMap<K, V> {

    private final Transaction tx;
    private final ObjId id;
    private final MapField<K, V> field;

    /**
     * Primary constructor.
     */
    JSMap(Transaction tx, MapField<K, V> field, ObjId id) {
        super(tx.kvt, field.keyField.encoding, false, field.buildKey(id));
        this.tx = tx;
        this.id = id;
        this.field = field;
    }

    /**
     * Internal constructor.
     */
    private JSMap(Transaction tx, MapField<K, V> field, ObjId id,
      boolean reversed, KeyRange keyRange, KeyFilter keyFilter, Bounds<K> bounds) {
        super(tx.kvt, field.keyField.encoding, false, reversed, field.buildKey(id), keyRange, keyFilter, bounds);
        Preconditions.checkArgument(keyRange != null, "null keyRange");
        this.tx = tx;
        this.id = id;
        this.field = field;
    }

    @Override
    public V put(final K keyObj, final V valueObj) {
        final ByteData key;
        final ByteData value;
        try {
            key = this.encodeVisibleKey(keyObj, true);
            value = this.encodeValue(valueObj);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
              "can't add invalid key/value pair to %s: %s", this.field, e.getMessage()), e);
        }
        return this.tx.mutateAndNotify(this.id, () -> this.doPut(keyObj, valueObj, key, value));
    }

    private V doPut(final K keyObj, final V newValueObj, ByteData key, ByteData newValue) {

        // Check for deleted assignment of key and/or value
        if (this.field.keyField instanceof ReferenceField)
            this.tx.checkDeletedAssignment(this.id, (ReferenceField)this.field.keyField, (ObjId)keyObj);
        if (this.field.valueField instanceof ReferenceField)
            this.tx.checkDeletedAssignment(this.id, (ReferenceField)this.field.valueField, (ObjId)newValueObj);

        // Get old value, if any
        final ByteData oldValue = this.tx.kvt.get(key);
        final V oldValueObj;
        if (oldValue != null) {

            // Decode old value
            oldValueObj = this.decodeValue(new KVPair(key, oldValue));

            // Optimize if no change
            if (newValue.equals(oldValue))
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
            this.tx.addFieldChangeNotification(oldValue != null ?
              new MapFieldReplaceNotifier<>(this.field, this.id, keyObj, oldValueObj, newValueObj) :
              new MapFieldAddNotifier<>(this.field, this.id, keyObj, newValueObj));
        }

        // Done
        return oldValueObj;
    }

    @Override
    public V remove(Object keyObj) {
        final ByteData key = this.encodeVisibleKey(keyObj, false);
        if (key == null)
            return null;
        final K canonicalKey = this.keyEncoding.validate(keyObj);
        return this.tx.mutateAndNotify(this.id, () -> this.doRemove(canonicalKey, key));
    }

    private V doRemove(final K keyObj, ByteData key) {

        // Get old value, if any
        final ByteData oldValue = this.tx.kvt.get(key);
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
        if (!this.tx.disableListenerNotifications)
            this.tx.addFieldChangeNotification(new MapFieldRemoveNotifier<>(this.field, this.id, keyObj, valueObj));

        // Done
        return valueObj;
    }

    @Override
    public void clear() {
        if (this.keyFilter != null)
            throw new UnsupportedOperationException("clear() not supported when KeyFilter configured");
        this.tx.mutateAndNotify(this.id, this::doClear);
    }

    private void doClear() {

        // Optimize if already empty
        if (this.isEmpty())
            return;

        // If range is restricted and there are field monitors, use individual deletions so we get individual notifications
        if (!this.bounds.isUnbounded() && this.tx.hasFieldMonitor(this.id, this.field.storageId)) {
            for (Iterator<Map.Entry<K, V>> i = this.entrySet().iterator(); i.hasNext(); ) {
                i.next();
                i.remove();
            }
            return;
        }

        // Get key range
        final ByteData rangeMinKey = this.keyRange.getMin();
        final ByteData rangeMaxKey = this.keyRange.getMax();

        // Delete index entries
        if (this.field.keyField.indexed)
            this.field.removeIndexEntries(this.tx, this.id, this.field.keyField, rangeMinKey, rangeMaxKey);
        if (this.field.valueField.indexed)
            this.field.removeIndexEntries(this.tx, this.id, this.field.valueField, rangeMinKey, rangeMaxKey);

        // Delete content
        this.field.deleteContent(this.tx, rangeMinKey, rangeMaxKey);

        // Notify field monitors
        if (!this.tx.disableListenerNotifications)
            this.tx.addFieldChangeNotification(new MapFieldClearNotifier<>(this.field, this.id));
    }

    @Override
    protected NavigableMap<K, V> createSubMap(boolean newReversed,
      KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<K> newBounds) {
        return new JSMap<>(this.tx, this.field, this.id, newReversed, newKeyRange, newKeyFilter, newBounds);
    }

    private ByteData encodeValue(Object obj) {
        final ByteData.Writer writer = ByteData.newWriter();
        this.field.valueField.encoding.validateAndWrite(writer, obj);
        return writer.toByteData();
    }

    @Override
    protected V decodeValue(KVPair pair) {
        return this.field.valueField.encoding.read(pair.getValue().newReader());
    }
}
