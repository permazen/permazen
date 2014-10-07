
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.primitives.Ints;

import java.util.List;

/**
 * {@code int[]} primitive array type. Does not support null arrays.
 */
class IntegerArrayType extends IntegralArrayType<int[], Integer> {

    IntegerArrayType() {
       super(FieldTypeRegistry.INTEGER);
    }

    @Override
    protected int getArrayLength(int[] array) {
        return array.length;
    }

    @Override
    protected Integer getArrayElement(int[] array, int index) {
        return array[index];
    }

    @Override
    protected int[] createArray(List<Integer> elements) {
        return Ints.toArray(elements);
    }
}

