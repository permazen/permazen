
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.util.ByteData;

import java.util.NavigableSet;

final class ListFieldRemoveNotifier<E> extends ListFieldNotifier<E> {

    final int index;

    ByteData encodedOldValue;
    E oldValue;

    ListFieldRemoveNotifier(ListField<E> field, ObjId id, int index, E oldValue) {
        super(field, id);
        this.index = index;
        this.oldValue = oldValue;
    }

    // On-demand decoding of removed value
    ListFieldRemoveNotifier(ListField<E> field, ObjId id, int index, ByteData encodedOldValue) {
        super(field, id);
        assert encodedOldValue != null;
        this.index = index;
        this.encodedOldValue = encodedOldValue;
    }

    private E getOldValue() {
        if (this.encodedOldValue != null) {
            this.oldValue = this.field.elementField.encoding.read(this.encodedOldValue.newReader());
            this.encodedOldValue = null;
        }
        return this.oldValue;
    }

    @Override
    public void notify(Transaction tx, ListFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onListFieldRemove(tx, this.id, this.field, path, referrers, this.index, this.getOldValue());
    }
}
