
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

abstract class CollectionElementStorageInfo<C extends Collection<E>, E, P extends CollectionField<C, E>>
  extends ComplexSubFieldStorageInfo<E, P> {

    CollectionElementStorageInfo(P field) {
        super(field.elementField, field);
    }

    @Override
    void readAllNonNull(Transaction tx, ObjId id, Set<E> values, Predicate<? super E> filter) {
        for (E value : this.parentRepresentative.getValueInternal(tx, id)) {
            if (value != null && (filter == null || filter.test(value)))
                values.add(value);
        }
    }
}

