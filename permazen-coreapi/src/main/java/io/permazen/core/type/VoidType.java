
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

import org.dellroad.stuff.java.Primitive;

/**
 * {@link void} primitive type.
 *
 * <p>
 * Doesn't support any values.
 */
public class VoidType extends PrimitiveType<Void> {

    private static final long serialVersionUID = -1158051649344218848L;

    public VoidType() {
       super(Primitive.VOID);
    }

    @Override
    public Void read(ByteReader reader) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(ByteWriter writer, Void value) {
        throw new IllegalArgumentException("null value");
    }

    @Override
    public void skip(ByteReader reader) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Void convertNumber(Number value) {
        throw new UnsupportedOperationException();
    }
}

