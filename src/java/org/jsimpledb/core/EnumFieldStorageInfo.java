
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

// Object

    @Override
    public String toString() {
        return "enum field with identifiers " + this.identList;
    }

    @Override
    protected boolean equalsSimple(SimpleFieldStorageInfo obj) {
        final EnumFieldStorageInfo that = (EnumFieldStorageInfo)obj;
        return this.identList.equals(that.identList);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.identList.hashCode();
    }
}

