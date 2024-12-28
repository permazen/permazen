
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.Encoding;
import io.permazen.kv.KVPair;
import io.permazen.kv.KeyRange;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.UnsignedIntEncoder;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.stream.Collectors;

/**
 * {@link List} implementation for {@link ListField}s.
 */
class JSList<E> extends AbstractList<E> implements RandomAccess {

    private final Transaction tx;
    private final ObjId id;
    private final ListField<E> field;
    private final Encoding<E> elementType;
    private final ByteData contentPrefix;

// Constructors

    JSList(Transaction tx, ListField<E> field, ObjId id) {
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(field != null, "null field");
        Preconditions.checkArgument(id != null, "null id");
        this.tx = tx;
        this.field = field;
        this.id = id;
        this.elementType = this.field.elementField.encoding;
        this.contentPrefix = field.buildKey(id);
    }

// List API

    @Override
    public E get(int index) {

        // Find list entry
        final ByteData value = this.tx.kvt.get(this.buildKey(index));
        if (value == null)
            throw new IndexOutOfBoundsException(String.format("index = %d", index));

        // Decode list element
        return this.elementType.read(value.newReader());
    }

    @Override
    public int size() {

        // Find the last entry, if it exists
        final KVPair pair = this.tx.kvt.getAtMost(ByteUtil.getKeyAfterPrefix(this.contentPrefix), this.contentPrefix);
        if (pair == null)
            return 0;
        assert pair.getKey().startsWith(this.contentPrefix);

        // Decode index from key to get size
        return UnsignedIntEncoder.read(pair.getKey().newReader(this.contentPrefix.size())) + 1;
    }

    @Override
    public E set(final int index, final E elem) {
        return this.tx.mutateAndNotify(this.id, () -> this.doSet(index, elem));
    }

    @Override
    public CloseableIterator<E> iterator() {
        return new Iter();
    }

    private E doSet(final int index, final E newElem) {

        // Build new key and value
        final ByteData key = this.buildKey(index);
        final ByteData newValue = this.buildValue(newElem);

        // Get existing list entry at that index, if any
        final ByteData oldValue = this.tx.kvt.get(key);
        if (oldValue == null)
            throw new IndexOutOfBoundsException(String.format("index = %d", index));

        // Check for deleted assignement
        if (this.field.elementField instanceof ReferenceField)
            this.tx.checkDeletedAssignment(this.id, (ReferenceField)this.field.elementField, (ObjId)newElem);

        // Optimize if no change
        if (newValue.equals(oldValue))
            return newElem;

        // Decode previous entry
        final E oldElem = this.elementType.read(oldValue.newReader());

        // Update list content and index
        this.tx.kvt.put(key, newValue);
        if (this.field.elementField.indexed) {
            this.field.removeIndexEntry(this.tx, this.id, this.field.elementField, key, oldValue);
            this.field.addIndexEntry(this.tx, this.id, this.field.elementField, key, newValue);
        }

        // Notify field monitors
        if (!this.tx.disableListenerNotifications)
            this.tx.addFieldChangeNotification(new ListFieldReplaceNotifier<>(this.field, this.id, index, oldElem, newElem));

        // Return previous entry
        return oldElem;
    }

    @Override
    public void add(final int index, final E elem) {
        if (index < 0)
            throw new IndexOutOfBoundsException(String.format("index = %d", index));
        this.tx.mutateAndNotify(this.id, () -> this.doAddAll(index, Collections.singleton(elem)));
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends E> elems) {
        if (index < 0)
            throw new IndexOutOfBoundsException(String.format("index = %d", index));
        return this.tx.mutateAndNotify(this.id, () -> this.doAddAll(index, elems));
    }

    private boolean doAddAll(int index, Collection<? extends E> elems0) {

        // Copy array
        final ArrayList<E> elems = new ArrayList<>(elems0);
        final int numElems = elems.size();

        // Check for deleted assignement
        if (this.field.elementField instanceof ReferenceField) {
            for (E elem : elems)
                this.tx.checkDeletedAssignment(this.id, (ReferenceField)this.field.elementField, (ObjId)elem);
        }

        // Encode elements
        final ArrayList<ByteData> values = elems.stream()
          .map(this::buildValue)
          .collect(Collectors.toCollection(() -> new ArrayList<>(numElems)));

        // Make room for elements
        final int size = this.size();
        if (index < 0 || index > size || size + numElems < 0)
            throw new IndexOutOfBoundsException(String.format("index = %d, size = %d", index, size));
        this.shift(index, index + numElems, size);

        // Add entries
        for (int i = 0; i < numElems; i++) {
            final ByteData key = this.buildKey(index);
            final ByteData value = values.get(i);
            final E elem = elems.get(i);

            // Update list content and index
            this.tx.kvt.put(key, value);
            if (this.field.elementField.indexed)
                this.field.addIndexEntry(this.tx, this.id, this.field.elementField, key, value);

            // Notify field monitors
            if (!this.tx.disableListenerNotifications)
                this.tx.addFieldChangeNotification(new ListFieldAddNotifier<>(this.field, this.id, index, elem));

            // Advance index
            index++;
        }

        // Done
        return numElems > 0;
    }

    @Override
    public void clear() {
        this.tx.mutateAndNotify(this.id, this::doClear);
    }

    private void doClear() {

        // Check size
        if (this.isEmpty())
            return;

        // Delete index entries
        if (this.field.elementField.indexed)
            this.field.removeIndexEntries(this.tx, this.id, this.field.elementField);

        // Delete content
        this.field.deleteContent(this.tx, this.id);

        // Notify field monitors
        if (!this.tx.disableListenerNotifications)
            this.tx.addFieldChangeNotification(new ListFieldClearNotifier<>(this.field, this.id));
    }

    @Override
    public E remove(int index) {
        final E elem = this.get(index);
        this.removeRange(index, index + 1);
        return elem;
    }

    @Override
    protected void removeRange(final int min, final int max) {
        this.tx.mutateAndNotify(this.id, () -> this.doRemoveRange(min, max));
    }

    private void doRemoveRange(int min, int max) {

        // Optimize for clear()
        final int size = this.size();
        if (min == 0 && max == size) {
            this.doClear();
            return;
        }

        // Check bounds
        Objects.checkFromToIndex(min, max, size);

        // Delete index entries
        if (this.field.elementField.indexed)
            this.deleteIndexEntries(min, max);

        // Notify field monitors
        if (!this.tx.disableListenerNotifications) {
            for (int i = min; i < max; i++) {
                final ByteData oldValue = this.tx.kvt.get(this.buildKey(i));
                if (oldValue == null)
                    throw new InconsistentDatabaseException(String.format("list entry at index %d not found", i));
                this.tx.addFieldChangeNotification(new ListFieldRemoveNotifier<>(this.field, this.id, i, oldValue));
            }
        }

        // Shift
        this.shift(max, min, size);
    }

    // Shift a contiguous range of list elements; values created or removed are not handled
    private void shift(int from, int to, int size) {

        // Bump modification counter (structural modification)
        this.modCount++;

        // Plan direction of copy operation to avoid overwrites
        int len = size - from;
        int src;
        int dst;
        int step;
        if (to < from) {
            src = from;
            dst = to;
            step = 1;
        } else {
            src = from + len - 1;
            dst = to + len - 1;
            step = -1;
            len = size - from;
        }

        // Copy list elements; note we remove the old index entries but not the old list entries
        while (len-- > 0) {

            // Read value at src
            final ByteData srcKey = this.buildKey(src);
            final ByteData value = this.tx.kvt.get(srcKey);
            if (value == null)
                throw new InconsistentDatabaseException(String.format("list entry at index %d not found", src));

            // Delete index entry for value at src
            if (this.field.elementField.indexed)
                this.field.removeIndexEntry(this.tx, this.id, this.field.elementField, srcKey, value);

            // Write value to dst
            final ByteData dstKey = this.buildKey(dst);
            this.tx.kvt.put(dstKey, value);

            // Add index entry for value at dst
            if (this.field.elementField.indexed)
                this.field.addIndexEntry(this.tx, this.id, this.field.elementField, dstKey, value);

            // Advance
            src += step;
            dst += step;
        }

        // Delete the old list entries that did not get overwritten; only happens when deleting items
        if (to < from) {
            final int newSize = size - (from - to);
            final ByteData minKey = this.buildKey(newSize);
            final ByteData maxKey = ByteUtil.getKeyAfterPrefix(this.contentPrefix);
            this.tx.kvt.removeRange(minKey, maxKey);
        }
    }

    private void deleteIndexEntries(int min, int max) {
        assert this.field.elementField.indexed;
        for (int i = min; i < max; i++) {
            final ByteData key = this.buildKey(i);
            final ByteData value = this.tx.kvt.get(key);
            if (value == null)
                throw new InconsistentDatabaseException(String.format("list entry at index %d not found", i));
            this.field.removeIndexEntry(this.tx, this.id, this.field.elementField, key, value);
        }
    }

    private ByteData buildKey(int index) {
        if (index < 0)
            throw new IndexOutOfBoundsException(String.format("index = %d", index));
        final ByteData.Writer writer = ByteData.newWriter();
        writer.write(this.contentPrefix);
        UnsignedIntEncoder.write(writer, index);
        return writer.toByteData();
    }

    private ByteData buildValue(E elem) {
        final ByteData.Writer writer = ByteData.newWriter();
        try {
            this.elementType.validateAndWrite(writer, elem);
        } catch (IllegalArgumentException e) {
            final String invalidType = elem != null ? elem.getClass().getName() : "null";
            throw new IllegalArgumentException(String.format(
              "list containing %s can't hold values of type %s", this.elementType, invalidType), e);
        }
        return writer.toByteData();
    }

// Iter

    private class Iter implements CloseableIterator<E> {

        private CloseableIterator<KVPair> i;
        private boolean finished;
        private Integer removeIndex;

        Iter() {
            this.i = JSList.this.tx.kvt.getRange(KeyRange.forPrefix(JSList.this.contentPrefix));
        }

        @Override
        public void close() {
            this.i.close();
        }

        @Override
        public synchronized boolean hasNext() {
            if (this.finished)
                return false;
            if (!this.i.hasNext()) {
                this.finished = true;
                this.i.close();
                return false;
            }
            return true;
        }

        @Override
        public synchronized E next() {
            if (this.finished)
                throw new NoSuchElementException();
            final KVPair pair = this.i.next();
            final ByteData.Reader keyReader = pair.getKey().newReader(JSList.this.contentPrefix.size());
            this.removeIndex = UnsignedIntEncoder.read(keyReader);
            return JSList.this.elementType.read(pair.getValue().newReader());
        }

        @Override
        public synchronized void remove() {
            Preconditions.checkState(this.removeIndex != null);
            JSList.this.removeRange(this.removeIndex, this.removeIndex + 1);
            this.removeIndex = null;
        }
    }
}
