
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.core.type.BooleanEncoding;
import io.permazen.core.type.ByteEncoding;
import io.permazen.core.type.CharacterEncoding;
import io.permazen.core.type.DoubleEncoding;
import io.permazen.core.type.FloatEncoding;
import io.permazen.core.type.IntegerEncoding;
import io.permazen.core.type.LongEncoding;
import io.permazen.core.type.ObjIdEncoding;
import io.permazen.core.type.ShortEncoding;
import io.permazen.core.type.UnsignedIntEncoding;
import io.permazen.core.type.VoidEncoding;
import io.permazen.util.UnsignedIntEncoder;

/**
 * Some {@link Encoding}s that are used for various general encoding tasks.
 *
 * <p>
 * All of the encodings defined in the class do <b>not</b> support null values.
 */
public final class Encodings {

    /**
     * Encodes the {@code void} primitive type, using zero bits.
     *
     * <p>
     * Completely useless, except perhaps as an invalid sentinel value.
     */
    public static final VoidEncoding VOID = new VoidEncoding();

    /**
     * Encodes the {@code boolean} primitive type.
     */
    public static final BooleanEncoding BOOLEAN = new BooleanEncoding();

    /**
     * Encodes the {@code byte} primitive type.
     */
    public static final ByteEncoding BYTE = new ByteEncoding();

    /**
     * Encodes the {@code char} primitive type.
     */
    public static final CharacterEncoding CHARACTER = new CharacterEncoding();

    /**
     * Encodes the {@code short} primitive type.
     */
    public static final ShortEncoding SHORT = new ShortEncoding();

    /**
     * Encodes the {@code int} primitive type.
     */
    public static final IntegerEncoding INTEGER = new IntegerEncoding();

    /**
     * Encodes the {@code float} primitive type.
     */
    public static final FloatEncoding FLOAT = new FloatEncoding();

    /**
     * Encodes the {@code long} primitive type.
     */
    public static final LongEncoding LONG = new LongEncoding();

    /**
     * Encodes the {@code double} primitive type.
     */
    public static final DoubleEncoding DOUBLE = new DoubleEncoding();

    /**
     * Encodes {@link ObjId}s.
     */
    public static final ObjIdEncoding OBJ_ID = new ObjIdEncoding();

    /**
     * Encodes unsigned integers via {@link UnsignedIntEncoder}.
     */
    public static final UnsignedIntEncoding UNSIGNED_INT = new UnsignedIntEncoding();

    private Encodings() {
    }
}
