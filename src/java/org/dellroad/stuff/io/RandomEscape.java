
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.util.Random;

/**
 * Generates a stream of pseudo-random but predictable bytes, so our escape character conflicts with low probability.
 * Note: instances are not thread safe.
 */
class RandomEscape {

    private static final long RANDOM_SEED = 0xd9035f670baad234L;

    private final Random random = new Random(RANDOM_SEED);

    public int next() {
        byte[] buf = new byte[1];
        this.random.nextBytes(buf);
        return buf[0] & 0xff;
    }
}

