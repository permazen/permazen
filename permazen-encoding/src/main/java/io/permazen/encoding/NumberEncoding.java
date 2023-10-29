
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import org.dellroad.stuff.java.Primitive;

/**
 * Support superclass for the numeric primitive types.
 */
public abstract class NumberEncoding<T extends Number> extends PrimitiveEncoding<T> {

    private static final long serialVersionUID = -2635244612906090817L;

    protected NumberEncoding(Primitive<T> primitive) {
       super(primitive);
    }
}
