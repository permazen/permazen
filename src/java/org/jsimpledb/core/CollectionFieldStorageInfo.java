
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Collections;
import java.util.List;

abstract class CollectionFieldStorageInfo extends ComplexFieldStorageInfo {

    final SimpleFieldStorageInfo elementField;

    CollectionFieldStorageInfo(CollectionField<?, ?> field) {
        super(field);
        this.elementField = field.elementField.toStorageInfo();
    }

    @Override
    public List<SimpleFieldStorageInfo> getSubFields() {
        return Collections.singletonList(this.elementField);
    }

    public SimpleFieldStorageInfo getElementField() {
        return this.elementField;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final CollectionFieldStorageInfo that = (CollectionFieldStorageInfo)obj;
        return this.elementField.equals(that.elementField);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.elementField.hashCode();
    }
}

