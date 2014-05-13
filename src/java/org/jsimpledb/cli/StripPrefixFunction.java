
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.base.Function;

/**
 * Strips a prefix, which must match any input.
 */
public class StripPrefixFunction implements Function<String, String> {

    private final String prefix;

    public StripPrefixFunction(String prefix) {
        if (prefix == null)
            throw new IllegalArgumentException("null prefix");
        this.prefix = prefix;
    }

    @Override
    public String apply(String string) {
        if (string == null)
            throw new IllegalArgumentException("null string");
        if (!string.startsWith(this.prefix))
            throw new IllegalArgumentException("string `" + string + "' does not have prefix `" + this.prefix + "'");
        return string.substring(prefix.length());
    }
}

