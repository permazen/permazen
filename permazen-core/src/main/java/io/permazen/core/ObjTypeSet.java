
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.KeyFilter;
import io.permazen.kv.KeyRange;
import io.permazen.util.Bounds;
import io.permazen.util.ByteData;

import java.util.NavigableSet;

/**
 * Sets containing all objects of a given type, with the ability to {@link #remove remove()} elements,
 * which results in {@linkplain Transaction#delete object deletion}.
 */
final class ObjTypeSet extends EncodingSet<ObjId> {

    private final Transaction tx;

    /**
     * Constructor for a single object type.
     *
     * @param tx transaction
     * @param storageId object type storage ID
     */
    ObjTypeSet(Transaction tx, int storageId) {
        super(tx.kvt, Encodings.OBJ_ID, true, false, ByteData.empty(),
          ObjId.getKeyRange(storageId), null, new Bounds<>(ObjId.getMin(storageId), ObjId.getMin(storageId + 1)));
        this.tx = tx;
    }

    /**
     * Constructor for all object types in all schemas.
     *
     * @param tx transaction
     */
    ObjTypeSet(Transaction tx) {
        super(tx.kvt, Encodings.OBJ_ID, true, false, ByteData.empty(),
          null, tx.getSchemaBundle().getObjTypesKeyRanges(), new Bounds<>());
        this.tx = tx;
    }

    /**
     * Internal constructor.
     *
     * @param tx transaction
     * @param reversed whether ordering is reversed (implies {@code bounds} are also inverted)
     * @param prefix prefix of all keys
     * @param keyRange key range restriction; must at least restrict to {@code prefix}
     * @param keyFilter key filter restriction, or null for none
     * @param bounds range restriction
     */
    private ObjTypeSet(Transaction tx, boolean reversed,
      ByteData prefix, KeyRange keyRange, KeyFilter keyFilter, Bounds<ObjId> bounds) {
        super(tx.kvt, Encodings.OBJ_ID, true, reversed, prefix, keyRange, keyFilter, bounds);
        this.tx = tx;
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
