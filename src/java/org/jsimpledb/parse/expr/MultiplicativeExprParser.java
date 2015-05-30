
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

/**
 * Parses multiplicative expressions of the form {@code x & y}, {@code x / y}, and {@code x % y}.
 */
public class MultiplicativeExprParser extends BinaryExprParser {

    public static final MultiplicativeExprParser INSTANCE = new MultiplicativeExprParser();

    public MultiplicativeExprParser() {
        super(CastExprParser.INSTANCE, Op.MULTIPLY, Op.DIVIDE, Op.MODULO);
    }
}

