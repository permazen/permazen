
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.primitives.Bytes;

import java.util.List;

/**
 * {@code byte[]} primitive array type. Does not support null arrays.
 */
class ByteArrayType extends IntegralArrayType<byte[], Byte> {

    ByteArrayType() {
       super(FieldTypeRegistry.BYTE, byte[].class);
    }

    @Override
    protected int getArrayLength(byte[] array) {
        return array.length;
    }

    @Override
    protected Byte getArrayElement(byte[] array, int index) {
        return array[index];
    }

    @Override
    protected byte[] createArray(List<Byte> elements) {
        return Bytes.toArray(elements);
    }
}

