
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.jsck;

/**
 * Represents an invalid/unexpected key in a JSimpleDB key/value database.
 */
public class MissingKey extends Issue {

    public MissingKey(byte[] key, byte[] value) {
        this("missing key", key, value);
    }

    public MissingKey(String description, byte[] key, byte[] value) {
        super(description, key, null, value);
    }
}

