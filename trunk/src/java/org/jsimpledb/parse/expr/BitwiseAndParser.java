
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

public class BitwiseAndParser extends BinaryExprParser {

    public static final BitwiseAndParser INSTANCE = new BitwiseAndParser();

    public BitwiseAndParser() {
        super(EqualityParser.INSTANCE, Op.AND);
    }
}

