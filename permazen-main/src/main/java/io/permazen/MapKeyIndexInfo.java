
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.core.type.ReferenceFieldType;

/**
 * Represents an index on the {@code key} sub-field of a {@link JMapField}.
 */
class MapKeyIndexInfo extends ComplexSubFieldIndexInfo {

    MapKeyIndexInfo(JMapField jfield) {
        super(jfield.keyField);
    }

    @Override
    protected Iterable<?> iterateReferences(Transaction tx, ObjId id) {
        assert this.getFieldType() instanceof ReferenceFieldType;
        return tx.readMapField(id, this.getParentStorageId(), false).keySet();
    }
}

