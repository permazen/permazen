
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

/**
 * Strips a prefix, which must match any input.
 */
public class StripPrefixFunction implements Function<String, String> {

    private final String prefix;

    public StripPrefixFunction(String prefix) {
        Preconditions.checkArgument(prefix != null, "null prefix");
        this.prefix = prefix;
    }

    @Override
    public String apply(String string) {
        Preconditions.checkArgument(string != null, "null string");
        if (!string.startsWith(this.prefix))
            throw new IllegalArgumentException("string `" + string + "' does not have prefix `" + this.prefix + "'");
        return string.substring(prefix.length());
    }
}

