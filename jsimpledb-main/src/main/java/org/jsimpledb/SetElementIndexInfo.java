
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.core.type.ReferenceFieldType;

/**
 * Represents an index on the {@code element} sub-field of a {@link JSetField}.
 */
class SetElementIndexInfo extends ComplexSubFieldIndexInfo {

    SetElementIndexInfo(JSetField jfield) {
        super(jfield.elementField);
    }

    @Override
    protected Iterable<?> iterateReferences(Transaction tx, ObjId id) {
        assert this.getFieldType() instanceof ReferenceFieldType;
        return tx.readSetField(id, this.getParentStorageId(), false);
    }
}

