
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.core.type.BooleanType;
import io.permazen.core.type.ByteType;
import io.permazen.core.type.CharacterType;
import io.permazen.core.type.DoubleType;
import io.permazen.core.type.FloatType;
import io.permazen.core.type.IntegerType;
import io.permazen.core.type.LongType;
import io.permazen.core.type.ObjIdType;
import io.permazen.core.type.ShortType;
import io.permazen.core.type.UnsignedIntType;
import io.permazen.core.type.VoidType;

/**
 * Some {@link FieldType}s that are used for various general encoding tasks.
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
    public static final VoidType VOID = new VoidType();

    /**
     * Encodes the {@code boolean} primitive type.
     */
    public static final BooleanType BOOLEAN = new BooleanType();

    /**
     * Encodes the {@code byte} primitive type.
     */
    public static final ByteType BYTE = new ByteType();

    /**
     * Encodes the {@code char} primitive type.
     */
    public static final CharacterType CHARACTER = new CharacterType();

    /**
     * Encodes the {@code short} primitive type.
     */
    public static final ShortType SHORT = new ShortType();

    /**
     * Encodes the {@code int} primitive type.
     */
    public static final IntegerType INTEGER = new IntegerType();

    /**
     * Encodes the {@code float} primitive type.
     */
    public static final FloatType FLOAT = new FloatType();

    /**
     * Encodes the {@code long} primitive type.
     */
    public static final LongType LONG = new LongType();

    /**
     * Encodes the {@code double} primitive type.
     */
    public static final DoubleType DOUBLE = new DoubleType();

    /**
     * Encodes {@link ObjId}s.
     */
    public static final ObjIdType OBJ_ID = new ObjIdType();

    /**
     * Encodes unsigned integers via {@link io.permazen.util.UnsignedIntEncoder}.
     */
    public static final UnsignedIntType UNSIGNED_INT = new UnsignedIntType();

    private Encodings() {
    }
}
