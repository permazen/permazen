
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import java.util.ArrayList;
import java.util.Collections;

import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ByteUtilTest extends TestSupport {

    @Test
    public void testToDouble() {
        for (int i = 0; i < 1000; i++) {
            final byte[] key1 = new byte[this.random.nextInt(this.random.nextInt(12) + 1)];
            final byte[] key2 = new byte[this.random.nextInt(this.random.nextInt(12) + 1)];
            this.random.nextBytes(key1);
            this.random.nextBytes(key2);

            final double value1 = ByteUtil.toDouble(key1);
            final double value2 = ByteUtil.toDouble(key2);
            Assert.assertTrue(value1 >= 0.0 && value1 < 1.0, "bogus value " + value1);
            Assert.assertTrue(value2 >= 0.0 && value2 < 1.0, "bogus value " + value2);

            this.checkSameCompare(ByteUtil.compare(key1, key2), Double.compare(value1, value2));

            final byte[] key1b = ByteUtil.fromDouble(value1);
            final byte[] key2b = ByteUtil.fromDouble(value2);
            final double value1b = ByteUtil.toDouble(key1b);
            final double value2b = ByteUtil.toDouble(key2b);

            Assert.assertEquals(value1b, value1);
            Assert.assertEquals(value2b, value2);

            Assert.assertTrue(Math.abs(value1 - value1b) <= 0.0000001);
            Assert.assertTrue(Math.abs(value2 - value2b) <= 0.0000001);

            this.checkSameCompare(ByteUtil.compare(key1, key2), ByteUtil.compare(key1b, key2b));
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

