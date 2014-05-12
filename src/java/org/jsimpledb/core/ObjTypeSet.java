
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

import org.jsimpledb.util.Bounds;

/**
 * Sets containing all objects of a given type, with the ability to {@link #remove remove()} elements
 * which results in {@linkplain Transaction#delete object deletion}.
 */
final class ObjTypeSet extends FieldTypeSet<ObjId> {

    /**
     * Primary constructor.
     *
     * @param tx transaction
     * @param storageId object type storage ID
     */
    ObjTypeSet(Transaction tx, int storageId) {
        super(tx, FieldType.OBJ_ID, true, false, null, ObjId.getMin(storageId).getBytes(),
          ObjId.getMin(storageId + 1).getBytes(), new Bounds<ObjId>(ObjId.getMin(storageId), ObjId.getMin(storageId + 1)));
    }

    /**
     * Internal constructor.
     *
     * @param tx transaction
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param minKey minimum visible key (inclusive), or null for none
     * @param maxKey maximum visible key (exclusive), or null for none
     * @param bounds range restriction
     */
    private ObjTypeSet(Transaction tx, boolean reversed, byte[] prefix, byte[] minKey, byte[] maxKey, Bounds<ObjId> bounds) {
        super(tx, FieldType.OBJ_ID, true, reversed, prefix, minKey, maxKey, bounds);
    }

    @Override
    public boolean remove(Object obj) {
        if (!(obj instanceof ObjId))
            return false;
        final ObjId id = (ObjId)obj;
        if (!this.inRange(id.getBytes()))
            return false;
        return this.tx.delete(id);
    }

    @Override
    protected NavigableSet<ObjId> createSubSet(boolean newReversed, byte[] newMinKey, byte[] newMaxKey, Bounds<ObjId> newBounds) {
        return new ObjTypeSet(this.tx, newReversed, this.prefix, newMinKey, newMaxKey, newBounds);
    }
}

