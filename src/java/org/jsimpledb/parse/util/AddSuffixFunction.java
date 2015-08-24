
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

/**
 * Adds a suffix.
 */
public class AddSuffixFunction implements Function<String, String> {

    private final String suffix;

    public AddSuffixFunction(String suffix) {
        Preconditions.checkArgument(suffix != null, "null suffix");
        this.suffix = suffix;
    }

    @Override
    public String apply(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return string + this.suffix;
    }
}

