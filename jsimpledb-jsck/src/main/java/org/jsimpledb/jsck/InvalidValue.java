
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.jsck;

import org.jsimpledb.kv.KVPair;

/**
 * Represents an invalid/unexpected value in a JSimpleDB key/value database under an otherwise valid key.
 */
public class InvalidValue extends Issue {

    public InvalidValue(KVPair pair) {
        this(pair, null);
    }

    public InvalidValue(KVPair pair, byte[] newValue) {
        this(pair.getKey(), pair.getValue(), newValue);
    }

    public InvalidValue(byte[] key, byte[] oldValue) {
        this(key, oldValue, null);
    }

    public InvalidValue(byte[] key, byte[] oldValue, byte[] newValue) {
        this("invalid value", key, oldValue, newValue);
    }

    public InvalidValue(String description, byte[] key, byte[] oldValue, byte[] newValue) {
        super(description, key, oldValue, newValue);
    }
}

