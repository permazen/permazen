
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import org.dellroad.stuff.java.Primitive;

/**
 * Short type.
 */
class ShortType extends IntegralType<Short> {

    ShortType() {
       super(Primitive.SHORT);
    }

    @Override
    protected Short downCast(long value) {
        return (short)value;
    }
}

