
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

abstract class CollectionFieldStorageInfo<C extends Collection<E>, E> extends ComplexFieldStorageInfo<C> {

    final SimpleFieldStorageInfo<E> elementField;

    CollectionFieldStorageInfo(CollectionField<C, E> field) {
        super(field);
        this.elementField = field.elementField.toStorageInfo();
    }

    @Override
    public List<SimpleFieldStorageInfo<E>> getSubFields() {
        return Collections.singletonList(this.elementField);
    }

    public SimpleFieldStorageInfo<E> getElementField() {
        return this.elementField;
    }

    abstract AbstractCoreIndex getElementFieldIndex(Transaction tx);

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final CollectionFieldStorageInfo<?, ?> that = (CollectionFieldStorageInfo<?, ?>)obj;
        return this.elementField.equals(that.elementField);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.elementField.hashCode();
    }
}

