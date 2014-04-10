
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Collections;
import java.util.List;

abstract class CollectionFieldStorageInfo extends ComplexFieldStorageInfo {

    SimpleFieldStorageInfo elementField;

    CollectionFieldStorageInfo(CollectionField<?, ?> field) {
        super(field);
    }

    @Override
    public List<SimpleFieldStorageInfo> getSubFields() {
        return Collections.singletonList(this.elementField);
    }

    void setSubFields(List<SimpleFieldStorageInfo> subFieldInfos) {
        if (subFieldInfos.size() != 1)
            throw new IllegalArgumentException();
        this.elementField = subFieldInfos.get(0);
    }

    public SimpleFieldStorageInfo getElementField() {
        return this.elementField;
    }
}

