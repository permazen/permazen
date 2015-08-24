
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.util;

import com.google.common.base.Predicate;

/**
 * A {@link Predicate} that tests whether given {@link String}s have a specified prefix.
 */
public class PrefixPredicate implements Predicate<String> {

    private final String prefix;

    public PrefixPredicate(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public boolean apply(String string) {
        return this.prefix != null ? string.startsWith(this.prefix) : true;
    }
}

