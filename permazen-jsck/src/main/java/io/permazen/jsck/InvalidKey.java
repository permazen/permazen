
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.kv.KVPair;

/**
 * Represents an invalid/unexpected key in a Permazen key/value database.
 */
public class InvalidKey extends Issue {

    public InvalidKey(KVPair pair) {
        this(pair.getKey(), pair.getValue());
    }

    public InvalidKey(byte[] key, byte[] value) {
        super("invalid key", key, value, null);
    }

    public InvalidKey(String message, byte[] key, byte[] value) {
        super(message, key, value, null);
    }
}
