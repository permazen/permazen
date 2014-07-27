
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.util;

import com.google.common.base.Function;

/**
 * Adds a prefix.
 */
public class AddPrefixFunction implements Function<String, String> {

    private final String prefix;

    public AddPrefixFunction(String prefix) {
        if (prefix == null)
            throw new IllegalArgumentException("null prefix");
        this.prefix = prefix;
    }

    @Override
    public String apply(String string) {
        if (string == null)
            throw new IllegalArgumentException("null string");
        return this.prefix + string;
    }
}

