
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

class CounterFieldStorageInfo extends FieldStorageInfo {

    CounterFieldStorageInfo(CounterField field) {
        super(field, 0);
    }

    @Override
    public String toString() {
        return "counter field";
    }

    protected void verifySharedStorageId(FieldStorageInfo other) {
        // nothing else to check
    }
}

