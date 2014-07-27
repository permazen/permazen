
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

public class EqualityParser extends BinaryExprParser {

    public static final EqualityParser INSTANCE = new EqualityParser();

    public EqualityParser() {
        super(RelationalExprParser.INSTANCE, Op.EQUAL, Op.NOT_EQUAL);
    }
}

