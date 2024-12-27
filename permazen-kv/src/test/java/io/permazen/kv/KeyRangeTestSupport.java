
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import io.permazen.test.TestSupport;
import io.permazen.util.ByteData;

import java.util.Arrays;

/**
 * Test support superclass adding key range utility methods.
 */
public abstract class KeyRangeTestSupport extends TestSupport {

    protected KeyRange randomKeyRange() {
        while (true) {
            final ByteData b1 = this.randomBytes(false);
            final ByteData b2 = this.randomBytes(true);
            if (b2 == null || b1.compareTo(b2) <= 0)
                return new KeyRange(b1, b2);
        }
    }

    protected static KeyRange kr(String min, String max) {
        return new KeyRange(min != null ? b(min) : ByteData.empty(), b(max));
    }

    protected static KeyRanges krs(KeyRange... ranges) {
        return new KeyRanges(Arrays.asList(ranges));
    }

    protected String s(KVPair pair) {
        return pair != null ? ("[" + s(pair.getKey()) + ", " + s(pair.getValue()) + "]") : "null";
    }

    protected ByteData randomBytes(boolean allowNull) {
        return this.randomBytes(0, 6, allowNull);
    }

    protected ByteData randomBytes(int minLength, int maxLength, boolean allowNull) {
        if (allowNull && this.random.nextFloat() < 0.1f)
            return null;
        final byte[] bytes = new byte[minLength + this.random.nextInt(maxLength - minLength)];
        this.random.nextBytes(bytes);
        return ByteData.of(bytes);
    }

    protected static ByteData[] ba(String... sa) {
        final ByteData[] ba = new ByteData[sa.length];
        for (int i = 0; i < sa.length; i++)
            ba[i] = b(sa[i]);
        return ba;
    }

    protected static ByteData b(String s) {
        return s == null ? null : ByteData.fromHex(s);
    }

    protected static String s(ByteData b) {
        return b == null ? "null" : b.toHex();
    }
}
