
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

public class AdditiveExprParser extends BinaryExprParser {

    public static final AdditiveExprParser INSTANCE = new AdditiveExprParser();

    public AdditiveExprParser() {
        super(MultiplicativeExprParser.INSTANCE, Op.PLUS, Op.MINUS);
    }
}

