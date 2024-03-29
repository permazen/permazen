
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.encoding.Encoding;
import io.permazen.encoding.StringEncoding;
import io.permazen.encoding.UnsignedIntEncoding;
import io.permazen.util.UnsignedIntEncoder;

/**
 * A few {@link Encoding}s that are used for various general encoding tasks.
 *
 * <p>
 * All of the encodings defined in the class do <b>not</b> support null values.
 */
public final class Encodings {

    /**
     * Encodes {@link ObjId}s.
     */
    public static final ObjIdEncoding OBJ_ID = new ObjIdEncoding();

    /**
     * Encodes unsigned integers via {@link UnsignedIntEncoder}.
     *
     * <p>
     * Used (for example) for encoding list indexes.
     */
    public static final UnsignedIntEncoding UNSIGNED_INT = new UnsignedIntEncoding();

    /**
     * Encodes non-null {@link String}s.
     */
    public static final StringEncoding STRING = new StringEncoding();

    private Encodings() {
    }
}
