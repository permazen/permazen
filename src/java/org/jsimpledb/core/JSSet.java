
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NavigableSet;

import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteUtil;

/**
 * Implements the {@link NavigableSet} view of a {@link SetField}.
 */
class JSSet<E> extends FieldTypeSet<E> {

    private final ObjId id;
    private final SetField<E> field;

    /**
     * Primary constructor.
     */
    JSSet(Transaction tx, SetField<E> field, ObjId id) {
        super(tx, field.elementField.fieldType, false, field.buildKey(id));
        this.id = id;
        this.field = field;
    }

    /**
     * Internal constructor.
     */
    private JSSet(Transaction tx, SetField<E> field, ObjId id,
      boolean reversed, KeyRange keyRange, KeyFilter keyFilter, Bounds<E> bounds) {
        super(tx, field.elementField.fieldType, false, reversed, field.buildKey(id), keyRange, keyFilter, bounds);
        Preconditions.checkArgument(keyRange != null, "null keyRange");
        this.id = id;
        this.field = field;
    }

    @Override
    public boolean add(final E newValue) {
        final byte[] key;
        try {
            key = this.encodeVisible(newValue, true);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("can't add invalid value to " + this.field + ": " + e.getMessage(), e);
        }
        return this.tx.mutateAndNotify(this.id, new Transaction.Mutation<Boolean>() {
            @Override
            public Boolean mutate() {
                return JSSet.this.doAdd(newValue, key);
            }
        });
    }

    private boolean doAdd(final E newValue, byte[] key) {

        // Check if already added
        if (this.tx.kvt.get(key) != null)
            return false;

        // Add element and index entry
        this.tx.kvt.put(key, ByteUtil.EMPTY);
        if (this.field.elementField.indexed)
            this.field.addIndexEntry(this.tx, this.id, this.field.elementField, key, null);

        // Notify field monitors
        this.tx.addFieldChangeNotification(new SetFieldChangeNotifier() {
            @Override
            void notify(Transaction tx, SetFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
                listener.onSetFieldAdd(tx, this.getId(), JSSet.this.field, path, referrers, newValue);
            }
        });

        // Done
        return true;
    }

    @Override
    public void clear() {
        if (this.keyFilter != null)
            throw new UnsupportedOperationException("clear() not supported when KeyFilter configured");
        this.tx.mutateAndNotify(this.id, new Transaction.Mutation<Void>() {
            @Override
            public Void mutate() {
                JSSet.this.doClear();
                return null;
            }
        });
    }

    private void doClear() {

        // Optimize if already empty
        if (this.isEmpty())
            return;

        // If range is restricted and there are field monitors, use individual deletions so we get individual notifications
        if (!this.bounds.equals(new Bounds<E>()) && this.tx.hasFieldMonitor(this.id, this.field)) {
            for (Iterator<E> i = this.iterator(); i.hasNext(); ) {
                i.next();
                i.remove();
            }
            return;
        }

        // Get key range
        final byte[] rangeMinKey = this.keyRange.getMin();
        final byte[] rangeMaxKey = this.keyRange.getMax();

        // Delete index entries
        if (this.field.elementField.indexed)
            this.field.removeIndexEntries(this.tx, this.id, this.field.elementField, rangeMinKey, rangeMaxKey);

        // Delete content
        this.field.deleteContent(this.tx, rangeMinKey, rangeMaxKey);

        // Notify field monitors
        this.tx.addFieldChangeNotification(new SetFieldChangeNotifier() {
            @Override
            void notify(Transaction tx, SetFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
                listener.onSetFieldClear(tx, this.getId(), JSSet.this.field, path, referrers);
            }
        });
    }

    @Override
    public boolean remove(Object obj) {
        final byte[] key = this.encodeVisible(obj, false);
        if (key == null)
            return false;
        final E canonicalElement = this.fieldType.validate(obj);                    // should never throw exception
        return this.tx.mutateAndNotify(this.id, new Transaction.Mutation<Boolean>() {
            @Override
            public Boolean mutate() {
                return JSSet.this.doRemove(canonicalElement, key);
            }
        });
    }

    private boolean doRemove(final E oldValue, byte[] key) {

        // See if already removed
        if (this.tx.kvt.get(key) == null)
            return false;

        // Remove element and index entry
        this.tx.kvt.remove(key);
        if (this.field.elementField.indexed)
            this.field.removeIndexEntry(this.tx, this.id, this.field.elementField, key, null);

        // Notify field monitors
        this.tx.addFieldChangeNotification(new SetFieldChangeNotifier() {
            @Override
            void notify(Transaction tx, SetFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
                listener.onSetFieldRemove(tx, this.getId(), JSSet.this.field, path, referrers, oldValue);
            }
        });

        // Done
        return true;
    }

    @Override
    protected NavigableSet<E> createSubSet(boolean newReversed, KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<E> newBounds) {
        return new JSSet<E>(this.tx, this.field, this.id, newReversed, newKeyRange, newKeyFilter, newBounds);
    }

// SetFieldChangeNotifier

    private abstract class SetFieldChangeNotifier implements FieldChangeNotifier {

        @Override
        public int getStorageId() {
            return JSSet.this.field.storageId;
        }

        @Override
        public ObjId getId() {
            return JSSet.this.id;
        }

        @Override
        public void notify(Transaction tx, Object listener, int[] path, NavigableSet<ObjId> referrers) {
            this.notify(tx, (SetFieldChangeListener)listener, path, referrers);
        }

        abstract void notify(Transaction tx, SetFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers);
    }
}

