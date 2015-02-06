
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

/**
 * Parses equality expressions of the form {@code x == y} or {@code x != y}.
 */
public class EqualityParser extends BinaryExprParser {

    public static final EqualityParser INSTANCE = new EqualityParser();

    public EqualityParser() {
        super(RelationalExprParser.INSTANCE, Op.EQUAL, Op.NOT_EQUAL);
    }
}

