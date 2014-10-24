
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import org.jsimpledb.core.CollectionField;

abstract class JCollectionFieldInfo extends JComplexFieldInfo {

    JCollectionFieldInfo(JCollectionField jfield) {
        super(jfield);
    }

    /**
     * Get the element sub-field info.
     */
    public JSimpleFieldInfo getElementFieldInfo() {
        return this.getSubFieldInfos().get(0);
    }

    @Override
    public String getSubFieldInfoName(JSimpleFieldInfo subFieldInfo) {
        if (subFieldInfo.storageId == this.getElementFieldInfo().getStorageId())
            return CollectionField.ELEMENT_FIELD_NAME;
        throw new RuntimeException("internal error");
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final JCollectionFieldInfo that = (JCollectionFieldInfo)obj;
        return this.getElementFieldInfo().equals(that.getElementFieldInfo());
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.getElementFieldInfo().hashCode();
    }
}

