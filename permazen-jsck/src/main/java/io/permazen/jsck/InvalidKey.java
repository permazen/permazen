
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.kv.KVPair;
import io.permazen.util.ByteData;

/**
 * Represents an invalid/unexpected key in a Permazen key/value database.
 */
public class InvalidKey extends Issue {

    public InvalidKey(KVPair pair) {
        this(pair.getKey(), pair.getValue());
    }

    public InvalidKey(ByteData key, ByteData value) {
        super("invalid key", key, value, null);
    }

    public InvalidKey(String message, ByteData key, ByteData value) {
        super(message, key, value, null);
    }
}
