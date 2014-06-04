
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import org.jsimpledb.core.EnumValue;

@SuppressWarnings("serial")
class UnmatchedEnumException extends JSimpleDBException {

    private final Class<? extends Enum<?>> type;
    private final EnumValue value;

    public UnmatchedEnumException(Class<? extends Enum<?>> type, EnumValue value) {
        super("no value found in Enum " + type + " matching " + value);
        this.type = type;
        this.value = value;
    }

    public Class<? extends Enum<?>> getType() {
        return this.type;
    }

    public EnumValue getValue() {
        return this.value;
    }
}

