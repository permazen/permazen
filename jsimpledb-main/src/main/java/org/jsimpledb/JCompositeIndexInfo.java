
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.ArrayList;
import java.util.List;

class JCompositeIndexInfo {

    final int storageId;
    final ArrayList<JSimpleFieldInfo> jfieldInfos;

    JCompositeIndexInfo(JCompositeIndex index) {
        this.storageId = index.storageId;
        this.jfieldInfos = new ArrayList<>(index.jfields.size());
    }

    public List<JSimpleFieldInfo> getJFieldInfos() {
        return this.jfieldInfos;
    }

// Object

    @Override
    public String toString() {
        return "composite index on " + this.jfieldInfos;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final JCompositeIndexInfo that = (JCompositeIndexInfo)obj;
        return this.storageId == that.storageId && this.jfieldInfos.equals(that.jfieldInfos);
    }

    @Override
    public int hashCode() {
        return this.storageId ^ this.jfieldInfos.hashCode();
    }
}

