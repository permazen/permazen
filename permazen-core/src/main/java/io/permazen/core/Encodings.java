
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.core.encoding.BooleanEncoding;
import io.permazen.core.encoding.Encoding;
import io.permazen.core.encoding.UnsignedIntEncoding;
import io.permazen.util.UnsignedIntEncoder;

/**
 * A few {@link Encoding}s that are used for various general encoding tasks.
 *
 * <p>
 * All of the encodings defined in the class do <b>not</b> support null values.
 */
public final class Encodings {

    /**
     * Encodes the "delete notified" flag in object meta-data.
     */
    public static final BooleanEncoding BOOLEAN = new BooleanEncoding();

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

    private Encodings() {
    }
}
