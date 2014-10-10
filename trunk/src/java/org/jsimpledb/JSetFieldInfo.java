
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import java.util.Deque;

import org.jsimpledb.core.ObjId;

class JSetFieldInfo extends JCollectionFieldInfo {

    JSetFieldInfo(JSetField jfield) {
        super(jfield);
    }

    @Override
    public NavigableSetConverter<?, ?> getConverter(JTransaction jtx) {
        final Converter<?, ?> elementConverter = this.elementFieldInfo.getConverter(jtx);
        return elementConverter != null ? this.createConverter(elementConverter) : null;
    }

    // This method exists solely to bind the generic type parameters
    private <X, Y> NavigableSetConverter<X, Y> createConverter(Converter<X, Y> elementConverter) {
        return new NavigableSetConverter<X, Y>(elementConverter);
    }

    @Override
    public void copyRecurse(ObjIdSet seen, JTransaction srcTx, JTransaction dstTx,
      ObjId id, int storageId, Deque<Integer> nextFields) {
        if (storageId != this.elementFieldInfo.storageId)
            throw new RuntimeException("internal error");
        this.copyRecurse(seen, srcTx, dstTx, srcTx.tx.readSetField(id, this.storageId, false), nextFields);
    }

// Object

    @Override
    public String toString() {
        return "set " + super.toString();
    }
}

