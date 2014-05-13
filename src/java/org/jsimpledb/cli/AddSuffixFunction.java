
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.base.Function;

/**
 * Adds a suffix.
 */
public class AddSuffixFunction implements Function<String, String> {

    private final String suffix;

    public AddSuffixFunction(String suffix) {
        if (suffix == null)
            throw new IllegalArgumentException("null suffix");
        this.suffix = suffix;
    }

    @Override
    public String apply(String string) {
        if (string == null)
            throw new IllegalArgumentException("null string");
        return string + this.suffix;
    }
}

