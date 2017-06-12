
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.core.type.ReferenceFieldType;

/**
 * Represents an index on the {@code element} sub-field of a {@link JListField}.
 */
class ListElementIndexInfo extends ComplexSubFieldIndexInfo {

    ListElementIndexInfo(JListField jfield) {
        super(jfield.elementField);
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Object toIndex(JTransaction jtx) {
        return new ConvertedIndex2(jtx.tx.queryListElementIndex(this.storageId),
          this.getConverter(jtx), jtx.referenceConverter, Converter.identity());
    }

    @Override
    protected Iterable<?> iterateReferences(Transaction tx, ObjId id) {
        assert this.getFieldType() instanceof ReferenceFieldType;
        return tx.readListField(id, this.getParentStorageId(), false);
    }
}

