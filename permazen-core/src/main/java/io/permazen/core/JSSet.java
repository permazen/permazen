
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.util.Bounds;
import io.permazen.util.ByteData;

import java.util.Iterator;
import java.util.NavigableSet;

/**
 * Implements the {@link NavigableSet} view of a {@link SetField}.
 */
class JSSet<E> extends EncodingSet<E> {

    private final Transaction tx;
    private final ObjId id;
    private final SetField<E> field;

    /**
     * Primary constructor.
     */
    JSSet(Transaction tx, SetField<E> field, ObjId id) {
        super(tx.kvt, field.elementField.encoding, false, field.buildKey(id));
        this.tx = tx;
        this.id = id;
        this.field = field;
    }

    /**
     * Internal constructor.
     */
    private JSSet(Transaction tx, SetField<E> field, ObjId id,
      boolean reversed, KeyRange keyRange, KeyFilter keyFilter, Bounds<E> bounds) {
        super(tx.kvt, field.elementField.encoding, false, reversed, field.buildKey(id), keyRange, keyFilter, bounds);
        Preconditions.checkArgument(keyRange != null, "null keyRange");
        this.tx = tx;
        this.id = id;
        this.field = field;
    }

    @Override
    public boolean add(final E newValue) {
        final ByteData key;
        try {
            key = this.encodeVisible(newValue, true);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("can't add invalid value to %s: %s", this.field, e.getMessage()), e);
        }
        return this.tx.mutateAndNotify(this.id, () -> this.doAdd(newValue, key));
    }

    private boolean doAdd(final E newValue, ByteData key) {

        // Check for deleted assignement
        if (this.field.elementField instanceof ReferenceField)
            this.tx.checkDeletedAssignment(this.id, (ReferenceField)this.field.elementField, (ObjId)newValue);

        // Check if already added
        if (this.tx.kvt.get(key) != null)
            return false;

        // Add element and index entry
        this.tx.kvt.put(key, ByteData.empty());
        if (this.field.elementField.indexed)
            this.field.addIndexEntry(this.tx, this.id, this.field.elementField, key, null);

        // Notify field monitors
        if (!this.tx.disableListenerNotifications)
            this.tx.addFieldChangeNotification(new SetFieldAddNotifier<>(this.field, this.id, newValue));

        // Done
        return true;
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
            for (Iterator<E> i = this.iterator(); i.hasNext(); ) {
                i.next();
                i.remove();
            }
            return;
        }

        // Get key range
        final ByteData rangeMinKey = this.keyRange.getMin();
        final ByteData rangeMaxKey = this.keyRange.getMax();

        // Delete index entries
        if (this.field.elementField.indexed)
            this.field.removeIndexEntries(this.tx, this.id, this.field.elementField, rangeMinKey, rangeMaxKey);

        // Delete content
        this.field.deleteContent(this.tx, rangeMinKey, rangeMaxKey);

        // Notify field monitors
        if (!this.tx.disableListenerNotifications)
            this.tx.addFieldChangeNotification(new SetFieldClearNotifier<>(this.field, this.id));
    }

    @Override
    public boolean remove(Object obj) {
        final ByteData key = this.encodeVisible(obj, false);
        if (key == null)
            return false;
        final E canonicalElement = this.encoding.validate(obj);                    // should never throw exception
        return this.tx.mutateAndNotify(this.id, () -> this.doRemove(canonicalElement, key));
    }

    private boolean doRemove(final E oldValue, ByteData key) {

        // See if already removed
        if (this.tx.kvt.get(key) == null)
            return false;

        // Remove element and index entry
        this.tx.kvt.remove(key);
        if (this.field.elementField.indexed)
            this.field.removeIndexEntry(this.tx, this.id, this.field.elementField, key, null);

        // Notify field monitors
        if (!this.tx.disableListenerNotifications)
            this.tx.addFieldChangeNotification(new SetFieldRemoveNotifier<>(this.field, this.id, oldValue));

        // Done
        return true;
    }

    @Override
    protected NavigableSet<E> createSubSet(boolean newReversed, KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<E> newBounds) {
        return new JSSet<>(this.tx, this.field, this.id, newReversed, newKeyRange, newKeyFilter, newBounds);
    }
}
