
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPairIterator;
import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteData;
import io.permazen.util.UnsignedIntEncoder;

import java.util.List;

/**
 * An index on the elements of a list field.
 *
 * @param <E> list element type
 */
public class ListElementIndex<E> extends CollectionElementIndex<List<E>, E> {

// Constructor

    ListElementIndex(Schema schema, SimpleSchemaField schemaField, ObjType objType, ListField<E> field) {
        super(schema, schemaField, objType, field);
    }

// Public Methods

    /**
     * Get this index's view of the given transaction, including list index.
     *
     * <p>
     * The returned index includes the list elements and their index positions in the list.
     *
     * @param tx transaction
     * @return view of this index in {@code tx}
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public CoreIndex2<E, ObjId, Integer> getElementIndex(Transaction tx) {
        return new CoreIndex2<>(tx.kvt,
          new Index2View<>(this.storageId, this.getEncoding(), Encodings.OBJ_ID, Encodings.UNSIGNED_INT));
    }

// IndexSwitch

    @Override
    public <R> R visit(IndexSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseListElementIndex(this);
    }

// Package Methods

    @Override
    boolean isPrefixModeForIndex() {
        return true;
    }

    // Note: as we delete list elements, the index of remaining elements will decrease by one each time.
    // However, the KVPairIterator always reflects the current state so we'll see updated list indexes.
    @Override
    void unreference(Transaction tx, boolean remove, ObjId target, ObjId referrer, ByteData prefix) {
        final List<?> list = tx.readListField(referrer, this.getField().parent.name, false);
        for (KVPairIterator i = new KVPairIterator(tx.kvt, prefix); i.hasNext(); ) {
            final ByteData.Reader reader = i.next().getKey().newReader();
            reader.skip(prefix.size());
            final int listIndex = UnsignedIntEncoder.read(reader);
            if (remove)
                list.remove(listIndex);
            else
                list.set(listIndex, null);
        }
    }
}
