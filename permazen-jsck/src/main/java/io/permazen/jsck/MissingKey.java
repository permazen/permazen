
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.util.ByteData;

/**
 * Represents a missing key in a Permazen key/value database.
 */
public class MissingKey extends Issue {

    public MissingKey(ByteData key, ByteData value) {
        this("missing key", key, value);
    }

    public MissingKey(String description, ByteData key, ByteData value) {
        super(description, key, null, value);
    }
}
