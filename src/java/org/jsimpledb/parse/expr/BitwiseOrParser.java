
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

/**
 * Parses bit-wise OR expressions of the form {@code x | y}. Also supports {@link java.util.Set} union.
 */
public class BitwiseOrParser extends BinaryExprParser {

    public static final BitwiseOrParser INSTANCE = new BitwiseOrParser();

    public BitwiseOrParser() {
        super(BitwiseXorParser.INSTANCE, Op.OR);
    }
}

