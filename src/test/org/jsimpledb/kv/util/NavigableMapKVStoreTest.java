
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.jsimpledb.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NavigableMapKVStoreTest extends TestSupport {

    @Test
    private void testEncodeDecode() throws Exception {
        for (int i = 0; i < 1000; i++) {

            // Populate k/v store with random data
            final NavigableMapKVStore original = new NavigableMapKVStore();
            for (int j = 0; j < this.random.nextInt(100); j++) {
                final byte[] key = this.rb(1 << this.random.nextInt(6));
                final byte[] val = this.rb(1 << this.random.nextInt(12));
                original.put(key, val);
            }

            // Encode then decode
            final ByteArrayOutputStream buf = new ByteArrayOutputStream();
            final long length = original.encodedLength();
            original.encode(buf);
            Assert.assertEquals(buf.size(), length);
            final NavigableMapKVStore copy = NavigableMapKVStore.decode(new ByteArrayInputStream(buf.toByteArray()));
            Assert.assertEquals(copy.getNavigableMap(), original.getNavigableMap());
        }
    }

    private byte[] rb(int len) {
        final byte[] b = new byte[this.random.nextInt(len) + 1];
        this.random.nextBytes(b);
        return b;
    }
}

