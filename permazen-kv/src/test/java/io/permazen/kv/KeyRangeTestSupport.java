
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import io.permazen.test.TestSupport;
import io.permazen.util.ByteUtil;

import java.util.Arrays;

/**
 * Test support superclass adding key range utility methods.
 */
public abstract class KeyRangeTestSupport extends TestSupport {

    protected KeyRange randomKeyRange() {
        while (true) {
            final byte[] b1 = this.randomBytes(false);
            final byte[] b2 = this.randomBytes(true);
            if (b2 == null || ByteUtil.compare(b1, b2) <= 0)
                return new KeyRange(b1, b2);
        }
    }

    protected static KeyRange kr(String min, String max) {
        return new KeyRange(min != null ? b(min) : ByteUtil.EMPTY, b(max));
    }

    protected static KeyRanges krs(KeyRange... ranges) {
        return new KeyRanges(Arrays.asList(ranges));
    }

    protected String s(KVPair pair) {
        return pair != null ? ("[" + s(pair.getKey()) + ", " + s(pair.getValue()) + "]") : "null";
    }
}
