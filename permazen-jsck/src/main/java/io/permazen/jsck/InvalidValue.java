
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.kv.KVPair;
import io.permazen.util.ByteData;

/**
 * Represents an invalid/unexpected value in a Permazen key/value database under an otherwise valid key.
 */
public class InvalidValue extends Issue {

    public InvalidValue(KVPair pair) {
        this(pair, null);
    }

    public InvalidValue(KVPair pair, ByteData newValue) {
        this(pair.getKey(), pair.getValue(), newValue);
    }

    public InvalidValue(ByteData key, ByteData oldValue) {
        this(key, oldValue, null);
    }

    public InvalidValue(ByteData key, ByteData oldValue, ByteData newValue) {
        this("invalid value", key, oldValue, newValue);
    }

    public InvalidValue(String description, ByteData key, ByteData oldValue, ByteData newValue) {
        super(description, key, oldValue, newValue);
    }
}
