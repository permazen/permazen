
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.Collections;
import java.util.List;

import org.jsimpledb.core.CollectionField;

abstract class JCollectionFieldInfo extends JComplexFieldInfo {

    final JSimpleFieldInfo elementFieldInfo;

    JCollectionFieldInfo(JCollectionField jfield) {
        super(jfield);
        this.elementFieldInfo = jfield.getElementField().toJFieldInfo();
    }

    /**
     * Get the element sub-field info.
     */
    public JSimpleFieldInfo getElementFieldInfo() {
        return this.elementFieldInfo;
    }

    @Override
    public List<JSimpleFieldInfo> getSubFieldInfos() {
        return Collections.singletonList(this.elementFieldInfo);
    }

    @Override
    public String getSubFieldInfoName(JSimpleFieldInfo subFieldInfo) {
        if (subFieldInfo.storageId == this.elementFieldInfo.storageId)
            return CollectionField.ELEMENT_FIELD_NAME;
        throw new RuntimeException("internal error");
    }

// Object

    @Override
    public String toString() {
        return super.toString() + " and element " + this.elementFieldInfo;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final JCollectionFieldInfo that = (JCollectionFieldInfo)obj;
        return this.elementFieldInfo.equals(that.elementFieldInfo);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.elementFieldInfo.hashCode();
    }
}

