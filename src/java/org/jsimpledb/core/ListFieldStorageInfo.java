
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.List;

import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.UnsignedIntEncoder;

class ListFieldStorageInfo extends CollectionFieldStorageInfo {

    ListFieldStorageInfo(ListField<?> field) {
        super(field);
    }

    // Note: as we delete list elements, the index of remaining elements will decrease by one each time.
    // However, the KVPairIterator always reflects the current state so we'll see updated indexes.
    @Override
    void unreference(Transaction tx, int storageId, ObjId target, ObjId referrer, byte[] prefix) {
        assert storageId == this.elementField.storageId;
        final List<?> list = tx.readListField(referrer, this.storageId, false);
        for (KVPairIterator i = new KVPairIterator(tx.kvt, prefix); i.hasNext(); ) {
            final ByteReader reader = new ByteReader(i.next().getKey());
            reader.skip(prefix.length);
            list.remove(UnsignedIntEncoder.read(reader));
        }
    }

    @Override
    public String toString() {
        return "list field with element " + this.elementField;
    }
}

