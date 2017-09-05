
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

/**
 * Parses relational expressions of the form {@code x < y}, {@code x >= y}, etc.
 */
public class RelationalExprParser extends BinaryExprParser {

    public static final RelationalExprParser INSTANCE = new RelationalExprParser();

    public RelationalExprParser() {
        super(InstanceofParser.INSTANCE, Op.LTEQ, Op.GTEQ, Op.LT, Op.GT);
    }
}

