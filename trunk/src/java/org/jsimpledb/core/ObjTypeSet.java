
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

import org.jsimpledb.kv.KeyFilter;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.util.Bounds;
import org.jsimpledb.util.ByteUtil;

/**
 * Sets containing all objects of a given type, with the ability to {@link #remove remove()} elements,
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
        super(tx, FieldTypeRegistry.OBJ_ID, true, false, ByteUtil.EMPTY,
          ObjId.getKeyRange(storageId), null, new Bounds<ObjId>(ObjId.getMin(storageId), ObjId.getMin(storageId + 1)));
    }

    /**
     * Internal constructor.
     *
     * @param tx transaction
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param prefix prefix of all keys
     * @param keyRanges key range restrictions; must at least restrict to {@code prefix}
     * @param bounds range restriction
     */
    private ObjTypeSet(Transaction tx, boolean reversed,
      byte[] prefix, KeyRange keyRange, KeyFilter keyFilter, Bounds<ObjId> bounds) {
        super(tx, FieldTypeRegistry.OBJ_ID, true, reversed, prefix, keyRange, keyFilter, bounds);
    }

    @Override
    public boolean remove(Object obj) {
        if (!(obj instanceof ObjId))
            return false;
        final ObjId id = (ObjId)obj;
        if (!this.isVisible(id.getBytes()))
            return false;
        return this.tx.delete(id);
    }

    @Override
    protected NavigableSet<ObjId> createSubSet(boolean newReversed,
      KeyRange newKeyRange, KeyFilter newKeyFilter, Bounds<ObjId> newBounds) {
        return new ObjTypeSet(this.tx, newReversed, this.prefix, newKeyRange, newKeyFilter, newBounds);
    }
}

