
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.List;

class EnumFieldStorageInfo extends SimpleFieldStorageInfo {

    final List<String> identList;

    EnumFieldStorageInfo(EnumField field, int superFieldStorageId, List<String> identList) {
        super(field, superFieldStorageId);
        if (identList == null)
            throw new IllegalArgumentException("null identList");
        this.identList = identList;
    }

    @Override
    protected void verifySharedStorageId(SimpleFieldStorageInfo that0) {
        final EnumFieldStorageInfo that = (EnumFieldStorageInfo)that0;
        if (!this.identList.equals(that.identList))
            throw new IllegalArgumentException("enum identifier lists differ: " + this.identList + " != " + that.identList);
    }

    @Override
    public String toString() {
        return "enum field";
    }
}

