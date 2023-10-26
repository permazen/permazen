
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.KVPairIterator;
import io.permazen.util.ByteReader;
import io.permazen.util.UnsignedIntEncoder;

import java.util.List;

class ListElementStorageInfo<E> extends CollectionElementStorageInfo<List<E>, E, ListField<E>> {

    ListElementStorageInfo(ListField<E> field) {
        super(field);
    }

    CoreIndex2<E, ObjId, Integer> getElementIndex(Transaction tx) {
        return new CoreIndex2<>(tx.kvt,
          new Index2View<>(this.storageId, this.fieldType, Encodings.OBJ_ID, Encodings.UNSIGNED_INT));
    }

    @Override
    CoreIndex<E, ObjId> getIndex(Transaction tx) {
        return this.getElementIndex(tx).asIndex();
    }

    // Note: as we delete list elements, the index of remaining elements will decrease by one each time.
    // However, the KVPairIterator always reflects the current state so we'll see updated list indexes.
    @Override
    void unreference(Transaction tx, ObjId target, ObjId referrer, byte[] prefix) {
        final List<?> list = tx.readListField(referrer, this.parentRepresentative.storageId, false);
        for (KVPairIterator i = new KVPairIterator(tx.kvt, prefix); i.hasNext(); ) {
            final ByteReader reader = new ByteReader(i.next().getKey());
            reader.skip(prefix.length);
            list.remove(UnsignedIntEncoder.read(reader));
        }
    }

// Object

    @Override
    public String toString() {
        return "list element with " + this.fieldType;
    }
}
