
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.regex.Pattern;

import org.dellroad.stuff.string.ParseContext;

/**
 * Non-null {@link Pattern} type. Null values are not supported by this class.
 *
 * <b>Note:</b> equality is not consistent with {@link Pattern#equals}, which is not implemented.
 */
class PatternType extends StringEncodedType<Pattern> {

    PatternType() {
        super(Pattern.class);
    }

// FieldType

    @Override
    public Pattern fromString(ParseContext ctx) {
        final String regex = ctx.getInput();
        try {
            return Pattern.compile(regex);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid pattern `" + regex + "'", e);
        }
    }
}

