
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

import java.util.ArrayList;

import org.jsimpledb.TestSupport;
import org.jsimpledb.util.ByteUtil;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class KeyRangesTest extends TestSupport {

    @Test(dataProvider = "ranges")
    public void testKeyRanges(KeyRanges ranges) throws Exception {

        Assert.assertEquals(ranges, ranges.inverse().inverse());
        Assert.assertEquals(ranges.getKeyRanges(), ranges.inverse().inverse().getKeyRanges());
        Assert.assertEquals(ranges.union(ranges.inverse()), KeyRanges.FULL);

        Assert.assertTrue(ranges.union(ranges.inverse()).isFull());
        Assert.assertEquals(ranges.intersection(ranges.inverse()), KeyRanges.EMPTY);
        Assert.assertTrue(ranges.intersection(ranges.inverse()).isEmpty());

        final byte[] b1 = this.randomBytes(false);
        Assert.assertEquals(ranges.contains(b1), !ranges.inverse().contains(b1),
          "ranges " + ranges + " and inverse " + ranges.inverse() + " agree on containing " + s(b1));

    }

    @DataProvider(name = "ranges")
    private Object[][] getDBs() throws Exception {
        final ArrayList<Object[]> paramsList = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            final ArrayList<KeyRange> list = new ArrayList<>();
            final int numRanges = this.random.nextInt(10);
            for (int j = 0; j < numRanges; j++)
                list.add(this.randomKeyRange());
            paramsList.add(new Object[] { new KeyRanges(list) });
        }
        return paramsList.toArray(new Object[paramsList.size()][]);
    }

    private KeyRange randomKeyRange() {
        final byte[] b1 = this.randomBytes(true);
        final byte[] b2 = this.randomBytes(true);
        return b1 == null || b2 == null || ByteUtil.compare(b1, b2) <= 0 ? new KeyRange(b1, b2) : new KeyRange(b2, b1);
    }

    private byte[] randomBytes(boolean allowNull) {
        if (allowNull && this.random.nextFloat() < 0.1f)
            return null;
        final byte[] bytes = new byte[this.random.nextInt(6)];
        this.random.nextBytes(bytes);
        return bytes;
    }

    private static String s(byte[] b) {
        return b == null ? "null" : ByteUtil.toString(b);
    }
}

