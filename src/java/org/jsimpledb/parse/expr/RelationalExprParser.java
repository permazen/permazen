
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

/**
 * Parses relational expressions of the form {@code x < y}, {@code x >= y}, etc.
 */
public class RelationalExprParser extends BinaryExprParser {

    public static final RelationalExprParser INSTANCE = new RelationalExprParser();

    public RelationalExprParser() {
        super(InstanceofParser.INSTANCE, Op.LTEQ, Op.GTEQ, Op.LT, Op.GT);
    }
}

