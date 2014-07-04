
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

public class BitwiseXorParser extends BinaryExprParser {

    public static final BitwiseXorParser INSTANCE = new BitwiseXorParser();

    public BitwiseXorParser() {
        super(BitwiseAndParser.INSTANCE, Op.XOR);
    }
}

