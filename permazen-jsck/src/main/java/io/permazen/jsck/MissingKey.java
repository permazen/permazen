
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

/**
 * Represents a missing key in a Permazen key/value database.
 */
public class MissingKey extends Issue {

    public MissingKey(byte[] key, byte[] value) {
        this("missing key", key, value);
    }

    public MissingKey(String description, byte[] key, byte[] value) {
        super(description, key, null, value);
    }
}

