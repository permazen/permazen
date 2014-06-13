
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.NavigableSet;

import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;

abstract class ComplexFieldStorageInfo extends FieldStorageInfo {

    ComplexFieldStorageInfo(ComplexField<?> field) {
        super(field, 0);
        this.initializeSubFields(Lists.transform(field.getSubFields(), new Function<SimpleField<?>, SimpleFieldStorageInfo>() {
            @Override
            public SimpleFieldStorageInfo apply(SimpleField<?> subField) {
                return subField.toStorageInfo();
            }
        }));
    }

    /**
     * Get the sub-fields associated with this instance.
     */
    public abstract List<SimpleFieldStorageInfo> getSubFields();

    /**
     * Initialize the sub-fields associated with this instance.
     */
    abstract void initializeSubFields(List<SimpleFieldStorageInfo> subFieldInfos);

    @Override
    public boolean canShareStorageId(StorageInfo obj) {
        if (!super.canShareStorageId(obj))
            return false;
        final ComplexFieldStorageInfo that = (ComplexFieldStorageInfo)obj;
        final List<SimpleFieldStorageInfo> thisSubFields = this.getSubFields();
        final List<SimpleFieldStorageInfo> thatSubFields = that.getSubFields();
        if (thisSubFields.size() != thatSubFields.size())
            return false;
        for (int i = 0; i < thisSubFields.size(); i++) {
            if (!thisSubFields.get(i).canShareStorageId(thatSubFields.get(i)))
                return false;
        }
        return true;
    }

    /**
     * Find all objects in the given referring for in which the specified sub-field of this field references
     * the specified target and remove the corresponding entry/entries. Used to implement {@link DeleteAction#UNREFERENCE}.
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

