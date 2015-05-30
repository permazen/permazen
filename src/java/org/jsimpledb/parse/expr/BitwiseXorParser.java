
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

/**
 * Parses bit-wise XOR expressions of the form {@code x ^ y}. Also supports {@link java.util.Set} symmetric difference.
 */
public class BitwiseXorParser extends BinaryExprParser {

    public static final BitwiseXorParser INSTANCE = new BitwiseXorParser();

    public BitwiseXorParser() {
        super(BitwiseAndParser.INSTANCE, Op.XOR);
    }
}

