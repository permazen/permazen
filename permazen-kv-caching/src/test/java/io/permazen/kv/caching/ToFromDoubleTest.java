
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.caching;

import io.permazen.test.TestSupport;
import io.permazen.util.ByteData;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ToFromDoubleTest extends TestSupport {

    @Test
    public void testToDouble() {
        for (int i = 0; i < 1000; i++) {
            final byte[] data1 = new byte[this.random.nextInt(this.random.nextInt(12) + 1)];
            final byte[] data2 = new byte[this.random.nextInt(this.random.nextInt(12) + 1)];
            this.random.nextBytes(data1);
            this.random.nextBytes(data2);

            final ByteData key1 = ByteData.of(data1);
            final ByteData key2 = ByteData.of(data2);

            final double value1 = CachingKVStore.toDouble(key1);
            final double value2 = CachingKVStore.toDouble(key2);
            Assert.assertTrue(value1 >= 0.0 && value1 < 1.0, "bogus value " + value1);
            Assert.assertTrue(value2 >= 0.0 && value2 < 1.0, "bogus value " + value2);

            this.checkSameCompare(key1.compareTo(key2), Double.compare(value1, value2));

            final ByteData key1b = CachingKVStore.fromDouble(value1);
            final ByteData key2b = CachingKVStore.fromDouble(value2);
            final double value1b = CachingKVStore.toDouble(key1b);
            final double value2b = CachingKVStore.toDouble(key2b);

            Assert.assertEquals(value1b, value1);
            Assert.assertEquals(value2b, value2);

            Assert.assertTrue(Math.abs(value1 - value1b) <= 0.0000001);
            Assert.assertTrue(Math.abs(value2 - value2b) <= 0.0000001);

            this.checkSameCompare(key1.compareTo(key2), key1b.compareTo(key2b));
        }
    }

    private void checkSameCompare(int diff1, int diff2) {
        if (diff1 > 0)
            Assert.assertTrue(diff2 >= 0, "diff2 == " + diff2);
        else if (diff1 < 0)
            Assert.assertTrue(diff2 <= 0, "diff2 == " + diff2);
        else
            Assert.assertTrue(diff2 == 0, "diff2 == " + diff2);
    }
}
