
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

public class ShiftExprParser extends BinaryExprParser {

    public static final ShiftExprParser INSTANCE = new ShiftExprParser();

    public ShiftExprParser() {
        super(AdditiveExprParser.INSTANCE, Op.LSHIFT, Op.RSHIFT, Op.URSHIFT);
    }
}

