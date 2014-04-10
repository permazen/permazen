
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.primitives.Shorts;

import java.util.List;

/**
 * {@code short[]} primitive array type. Does not support null arrays.
 */
class ShortArrayType extends IntegralArrayType<short[], Short> {

    ShortArrayType() {
       super(FieldType.SHORT);
    }

    @Override
    protected int getArrayLength(short[] array) {
        return array.length;
    }

    @Override
    protected Short getArrayElement(short[] array, int index) {
        return array[index];
    }

    @Override
    protected short[] createArray(List<Short> elements) {
        return Shorts.toArray(elements);
    }
}

