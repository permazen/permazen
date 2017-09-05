
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

/**
 * Parses Java additive expressions of the form {@code x + y} and {@code x - y}.
 */
public class AdditiveExprParser extends BinaryExprParser {

    public static final AdditiveExprParser INSTANCE = new AdditiveExprParser();

    public AdditiveExprParser() {
        super(MultiplicativeExprParser.INSTANCE, Op.PLUS, Op.MINUS);
    }
}

