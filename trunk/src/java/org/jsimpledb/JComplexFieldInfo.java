
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.Deque;
import java.util.List;

import org.jsimpledb.core.ObjId;

abstract class JComplexFieldInfo extends JFieldInfo {

    JComplexFieldInfo(JComplexField jfield) {
        super(jfield);
    }

    public abstract List<JSimpleFieldInfo> getSubFieldInfos();

    public abstract String getSubFieldInfoName(JSimpleFieldInfo subFieldInfo);

    /**
     * Recurse for copying between transactions. Copies all objects referred to by any reference in the given
     * subfield of the given object from {@code srcTx} to {@code dstTx}.
     *
     * @param seen IDs of objects already copied
     * @param srcTx source transaction
     * @param dstTx destination transaction
     * @param id ID of the object containing this complex field in {@code srcTx}
     * @param storageId storage ID of the sub-field of this field containing the references to copy
     * @param nextFields remaining fields to follow in the reference path
     */
    public abstract void copyRecurse(ObjIdSet seen, JTransaction srcTx, JTransaction dstTx,
      ObjId id, int storageId, Deque<Integer> nextFields);

    // Recurse on the iteration of references
    protected void copyRecurse(ObjIdSet seen, JTransaction srcTx, JTransaction dstTx, Iterable<?> it, Deque<Integer> nextFields) {
        for (Object obj : it) {
            if (obj != null) {
                final ObjId id = (ObjId)obj;
                srcTx.copyTo(seen, dstTx, id, id, false, nextFields);
            }
        }
    }
}

