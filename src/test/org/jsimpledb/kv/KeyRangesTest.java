
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jsimpledb.TestSupport;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class KeyRangesTest extends TestSupport {

///////////// getKeyRanges()

    @Test(dataProvider = "minimal")
    public void testGetKeyRanges(KeyRanges ranges, List<KeyRange> minimal) throws Exception {
        Assert.assertEquals(ranges.getKeyRanges(), minimal);
    }

    @DataProvider(name = "minimal")
    private Object[][] dataGetKeyRanges() throws Exception {
        return new Object[][] {

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09"), kr("0a", null)),
                Arrays.asList(kr(null, "02"), kr("03", "05"), kr("07", "09"), kr("0a", null))
            },

            {
                krs(kr(null, "02"), kr("07", "09"), kr("03", "05"), kr("0a", null)),
                Arrays.asList(kr(null, "02"), kr("03", "05"), kr("07", "09"), kr("0a", null))
            },

            {
                krs(kr(null, "02"), kr("01", "03"), kr("02", "04"), kr("03", "05")),
                Arrays.asList(kr(null, "05"))
            },

            {
                krs(kr(null, "02"), kr("01", null)),
                Arrays.asList(kr(null, null))
            },

            {
                krs(kr("03", "05"), kr("05", "07")),
                Arrays.asList(kr("03", "07"))
            },

            {
                krs(kr("03", "03"), kr("05", "05")),
                Arrays.asList()
            },

            {
                krs(kr(null, "05"), kr("05", "07")),
                Arrays.asList(kr(null, "07"))
            },

            {
                krs(kr(null, "05"), kr("05", null)),
                Arrays.asList(kr(null, null))
            },

            {
                krs(kr(null, null), kr("05", "09")),
                Arrays.asList(kr(null, null))
            },

            {
                krs(kr("05", "09"), kr(null, null)),
                Arrays.asList(kr(null, null))
            },

            {
                krs(kr("05", "09"), kr(null, null), kr("01", "03")),
                Arrays.asList(kr(null, null))
            },

            {
                krs(kr("0101", null), kr("01", "0100")),
                Arrays.asList(kr("01", "0100"), kr("0101", null))
            },

        };
    }

///////////// getKeyRange()

    @Test(dataProvider = "getKeyRanges")
    public void testGetKeyRange(KeyRanges ranges, String keystr, int contains, int left, int right) throws Exception {
        final byte[] key = b(keystr);
        final List<KeyRange> list = ranges.getKeyRanges();
        final KeyRange[] neighbors = ranges.findKey(key);
        if (contains == -1)
            Assert.assertTrue(neighbors[0] != neighbors[1] || neighbors[0] == null);
        else
            Assert.assertTrue(neighbors[0] == neighbors[1] && list.get(contains).equals(neighbors[0]));
        Assert.assertEquals(neighbors[0], left != -1 ? list.get(left) : null);
        Assert.assertEquals(neighbors[1], right != -1 ? list.get(right) : null);
    }

    @DataProvider(name = "getKeyRanges")
    private Object[][] dataGetKeyRange() throws Exception {
        return new Object[][] {

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09")), "", 0, 0, 0
            },

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09")), "01", 0, 0, 0
            },

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09")), "01ffffffffffffffff", 0, 0, 0
            },

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09")), "02", -1, 0, 1
            },

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09")), "0280", -1, 0, 1
            },

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09")), "03", 1, 1, 1
            },

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09")), "05", -1, 1, 2
            },

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09")), "06", -1, 1, 2
            },

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09")), "07", 2, 2, 2
            },

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09")), "08", 2, 2, 2
            },

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09")), "09", -1, 2, -1
            },

            {
                krs(kr(null, "02"), kr("03", "05"), kr("07", "09")), "0900", -1, 2, -1
            },

        };
    }

///////////// inverse(), union(), intersection()

    @Test(dataProvider = "KeyRanges")
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

    @DataProvider(name = "KeyRanges")
    private Object[][] dataKeyRanges() throws Exception {
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
}

