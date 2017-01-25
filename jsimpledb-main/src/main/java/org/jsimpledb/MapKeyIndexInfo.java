
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;

/**
 * Represents an index on the {@code key} sub-field of a {@link JMapField}.
 */
class MapKeyIndexInfo extends ComplexSubFieldIndexInfo {

    MapKeyIndexInfo(JMapField jfield) {
        super(jfield.keyField);
    }

    @Override
    protected Iterable<?> iterateReferences(Transaction tx, ObjId id) {
        return tx.readMapField(id, this.getParentStorageId(), false).keySet();
    }
}

