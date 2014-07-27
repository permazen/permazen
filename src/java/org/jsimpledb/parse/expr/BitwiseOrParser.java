
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

public class BitwiseOrParser extends BinaryExprParser {

    public static final BitwiseOrParser INSTANCE = new BitwiseOrParser();

    public BitwiseOrParser() {
        super(BitwiseXorParser.INSTANCE, Op.OR);
    }
}

