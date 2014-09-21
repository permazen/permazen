
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import java.lang.reflect.Field;

/**
 * Value that reflects a Java field in some class or object.
 */
abstract class AbstractFieldValue extends AbstractValue implements LValue {

    protected final Field field;

    protected AbstractFieldValue(Field field) {
        if (field == null)
            throw new IllegalArgumentException("null field");
        this.field = field;
    }
}

