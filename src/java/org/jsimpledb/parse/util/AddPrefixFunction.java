
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

/**
 * Adds a prefix.
 */
public class AddPrefixFunction implements Function<String, String> {

    private final String prefix;

    public AddPrefixFunction(String prefix) {
        Preconditions.checkArgument(prefix != null, "null prefix");
        this.prefix = prefix;
    }

    @Override
    public String apply(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return this.prefix + string;
    }
}

