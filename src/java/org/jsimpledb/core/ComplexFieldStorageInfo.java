
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.List;
import java.util.NavigableSet;

import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;

abstract class ComplexFieldStorageInfo<T> extends FieldStorageInfo {

    ComplexFieldStorageInfo(ComplexField<T> field) {
        super(field);
    }

    /**
     * Get the sub-fields associated with this instance.
     */
    public abstract List<? extends SimpleFieldStorageInfo<?>> getSubFields();

    abstract CoreIndex<?, ObjId> getSimpleSubFieldIndex(Transaction tx, SimpleFieldStorageInfo<?> subField);

    /**
     * Find all objects in the given referrers set for which the specified sub-field of this field references
     * the specified target, and remove the corresponding entry/entries. Used to implement {@link DeleteAction#UNREFERENCE}.
     *
     * @param tx transaction
     * @param field sub-field of this field referencing target
     * @param target referenced object being deleted
     * @param referrers objects that refer to {@code target} via this field
     */
    void unreferenceAll(Transaction tx, int storageId, ObjId target, NavigableSet<ObjId> referrers) {

        // Construct index entry prefix common to all referrers
        final ByteWriter writer = new ByteWriter(1 + ObjId.NUM_BYTES * 2);      // common case for storage ID's < 0xfb
        UnsignedIntEncoder.write(writer, storageId);
        target.writeTo(writer);
        final int mark = writer.mark();

        // Iterate over referrers, extend index entry, and let sub-class do the rest
        for (ObjId referrer : referrers) {
            writer.reset(mark);
            referrer.writeTo(writer);
            this.unreference(tx, storageId, target, referrer, writer.getBytes());
        }
    }

    /**
     * Remove entries in the specified sub-field of this field in the specified referrer object to the specified target.
     * Used to implement {@link DeleteAction#UNREFERENCE}.
     *
     * @param tx transaction
     * @param storageId sub-field storage ID
     * @param target referenced object being deleted
     * @param referrer object containing this field which refers to {@code target}
     * @param prefix (possibly partial) index entry containing {@code target} and {@code referrer}
     */
    abstract void unreference(Transaction tx, int storageId, ObjId target, ObjId referrer, byte[] prefix);
}

