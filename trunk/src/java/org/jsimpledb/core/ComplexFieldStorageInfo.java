
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Iterator;
import java.util.List;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;

abstract class ComplexFieldStorageInfo extends FieldStorageInfo {

    ComplexFieldStorageInfo(ComplexField<?> field) {
        super(field, 0);
    }

    /**
     * Get the sub-fields associated with this instance.
     */
    public abstract List<SimpleFieldStorageInfo> getSubFields();

    /**
     * Set the sub-fields associated with this instance.
     */
    abstract void setSubFields(List<SimpleFieldStorageInfo> subFieldInfos);

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
     * Applies the {@link DeleteAction#UNREFERENCE} operation to all objects in which
     * the specified sub-field of this field references the specified target.
     *
     * @param tx transaction
     * @param storageId sub-field storage ID
     * @param target referenced object being deleted
     */
    void unreferenceAll(Transaction tx, int storageId, ObjId target) {
        final ByteWriter prefixWriter = new ByteWriter();
        UnsignedIntEncoder.write(prefixWriter, storageId);
        target.writeTo(prefixWriter);
        final byte[] prefix = prefixWriter.getBytes();
        final int prefixLen = prefix.length;
        final byte[] prefixEnd = ByteUtil.getKeyAfterPrefix(prefix);
        for (Iterator<KVPair> i = tx.kvt.getRange(prefix, prefixEnd, false); i.hasNext(); ) {
            final KVPair pair = i.next();
            final ByteReader indexEntryReader = new ByteReader(pair.getKey());
            indexEntryReader.skip(prefixLen);
            final ObjId referrer = new ObjId(indexEntryReader);
            this.unreference(tx, storageId, target, referrer, indexEntryReader);
        }
    }

    /**
     * Applies the {@link DeleteAction#UNREFERENCE} operation for the specified sub-field of this field in the specified object.
     *
     * @param tx transaction
     * @param storageId sub-field storage ID
     * @param target referenced object being deleted
     * @param referrer object containing this field which refers to {@code target}
     * @param reader index entry bytes, positioned after the referring object ID (i.e., {@code referrer})
     */
    abstract void unreference(Transaction tx, int storageId, ObjId target, ObjId referrer, ByteReader reader);
}

