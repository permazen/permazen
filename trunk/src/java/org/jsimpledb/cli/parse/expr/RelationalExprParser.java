
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

public class RelationalExprParser extends BinaryExprParser {

    public static final RelationalExprParser INSTANCE = new RelationalExprParser();

    public RelationalExprParser() {
        super(InstanceofParser.INSTANCE, Op.LTEQ, Op.GTEQ, Op.LT, Op.GT);
    }
}

