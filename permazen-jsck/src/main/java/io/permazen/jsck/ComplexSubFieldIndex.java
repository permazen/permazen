
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.ComplexField;
import io.permazen.core.SimpleField;

abstract class ComplexSubFieldIndex<
    C,
    PF extends ComplexField<C>,
    T,
    I extends io.permazen.core.ComplexSubFieldIndex<C, T>>
  extends SimpleIndex<T, I> {

    protected final PF parentField;

    protected ComplexSubFieldIndex(JsckInfo info, PF parentField, SimpleField<T> field) {
        super(info, field);
        this.parentField = parentField;
    }
}
