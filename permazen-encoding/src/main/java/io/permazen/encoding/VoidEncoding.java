
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

import java.util.OptionalInt;

import org.dellroad.stuff.java.Primitive;

/**
 * Encoding for {@code void} primitive type.
 *
 * <p>
 * Doesn't support any values.
 */
public class VoidEncoding extends PrimitiveEncoding<Void> {

    private static final long serialVersionUID = -1158051649344218848L;

    public VoidEncoding() {
       super(Primitive.VOID);
    }

    @Override
    public Void read(ByteReader reader) {
        return null;
    }

    @Override
    public void write(ByteWriter writer, Void value) {
    }

    @Override
    public void skip(ByteReader reader) {
    }

    @Override
    protected Void convertNumber(Number value) {
        throw new IllegalArgumentException();
    }

    @Override
    public boolean hasPrefix0x00() {
        return false;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public OptionalInt getFixedWidth() {
        return OptionalInt.of(0);
    }
}
