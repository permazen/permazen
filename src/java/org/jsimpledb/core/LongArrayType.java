
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.primitives.Longs;

import java.util.List;

/**
 * {@code long[]} primitive array type. Does not support null arrays.
 */
class LongArrayType extends IntegralArrayType<long[], Long> {

    LongArrayType() {
       super(FieldTypeRegistry.LONG, long[].class);
    }

    @Override
    protected int getArrayLength(long[] array) {
        return array.length;
    }

    @Override
    protected Long getArrayElement(long[] array, int index) {
        return array[index];
    }

    @Override
    protected long[] createArray(List<Long> elements) {
        return Longs.toArray(elements);
    }
}

