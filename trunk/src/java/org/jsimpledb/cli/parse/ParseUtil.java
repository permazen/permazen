
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse;

import com.google.common.collect.Iterables;

import org.jsimpledb.cli.util.AddSuffixFunction;
import org.jsimpledb.cli.util.PrefixPredicate;
import org.jsimpledb.cli.util.StripPrefixFunction;

/**
 * Parsing utility routines.
 */
public final class ParseUtil {

    private ParseUtil() {
    }

    /**
     * Truncate a string with ellipsis if necessary.
     */
    public static String truncate(String string, int len) {
        if (len < 4)
            throw new IllegalArgumentException("len = " + len + " < 4");
        if (string.length() <= len)
            return string;
        return string.substring(0, len - 3) + "...";
    }

    /**
     * Generate completions based on a set of possibilities and the provided input prefix.
     */
    public static Iterable<String> complete(Iterable<String> choices, String prefix) {
        return Iterables.transform(
          Iterables.transform(Iterables.filter(choices, new PrefixPredicate(prefix)), new StripPrefixFunction(prefix)),
        new AddSuffixFunction(" "));
    }
}

