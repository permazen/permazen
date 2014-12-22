
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.List;

class EnumFieldStorageInfo extends SimpleFieldStorageInfo<EnumValue> {

    EnumFieldStorageInfo(EnumField field, int superFieldStorageId) {
        super(field, superFieldStorageId);
    }

    public List<String> getIdentifiers() {
        return ((EnumFieldType)this.fieldType).getIdentifiers();
    }

// Object

    @Override
    public String toString() {
        return "enum field with identifiers " + this.getIdentifiers();
    }

    @Override
    protected boolean fieldTypeEquals(SimpleFieldStorageInfo<?> that0) {
        final EnumFieldStorageInfo that = (EnumFieldStorageInfo)that0;
        return this.getIdentifiers().equals(that.getIdentifiers());
    }

    @Override
    protected int fieldTypeHashCode() {
        return this.getIdentifiers().hashCode();
    }
}

