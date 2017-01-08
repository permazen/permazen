
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import org.dellroad.stuff.java.Primitive;

/**
 * Support superclass for the numeric primitive types.
 */
abstract class NumberType<T extends Number> extends PrimitiveType<T> {

    private static final long serialVersionUID = -2635244612906090817L;

    NumberType(Primitive<T> primitive) {
       super(primitive);
    }
}

