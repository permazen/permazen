
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.CollectionField;

import java.util.Collection;

abstract class CollectionElementIndex<
    C extends Collection<E>,
    PF extends CollectionField<C, E>,
    E,
    I extends io.permazen.core.CollectionElementIndex<C, E>>
  extends ComplexSubFieldIndex<C, PF, E, I> {

    protected CollectionElementIndex(JsckInfo info, PF collectionField) {
        super(info, collectionField, collectionField.getElementField());
    }
}
