
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

public class MultiplicativeExprParser extends BinaryExprParser {

    public static final MultiplicativeExprParser INSTANCE = new MultiplicativeExprParser();

    public MultiplicativeExprParser() {
        super(CastExprParser.INSTANCE, Op.MULTIPLY, Op.DIVIDE, Op.MODULO);
    }
}

