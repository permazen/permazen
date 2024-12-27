
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import io.permazen.util.ByteData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class KeyRangesTest extends KeyRangeTestSupport {

///////////// asList()

    @Test(dataProvider = "minimal")
    public void testMinimal(KeyRanges ranges, List<KeyRange> minimal) throws Exception {
        Assert.assertEquals(ranges.asList(), minimal);
    }

    @DataProvider(name = "minimal")
    private Object[][] dataMinimal() throws Exception {
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

    @Test(dataProvider = "asList")
    public void testAsList(KeyRanges ranges, String keystr, int contains, int left, int right) throws Exception {
        final ByteData key = b(keystr);
        final List<KeyRange> list = ranges.asList();
        final KeyRange[] neighbors = ranges.findKey(key);
        if (contains == -1)
            Assert.assertTrue(neighbors[0] != neighbors[1] || neighbors[0] == null);
        else
            Assert.assertTrue(neighbors[0] == neighbors[1] && list.get(contains).equals(neighbors[0]));
        Assert.assertEquals(neighbors[0], left != -1 ? list.get(left) : null);
        Assert.assertEquals(neighbors[1], right != -1 ? list.get(right) : null);
    }

    @DataProvider(name = "asList")
    private Object[][] dataAsList() throws Exception {
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

///////////// add(), remove()

    @Test(dataProvider = "removes")
    public void testRemove(KeyRange range, KeyRanges expected) throws Exception {

        final KeyRanges kr1 = KeyRanges.empty();
        kr1.add(kr("01", "02"));
        kr1.add(kr("03", "04"));
        kr1.add(kr("0400", "06"));
        kr1.add(kr("09", "0a"));
        kr1.add(kr("0b", null));

        kr1.remove(range);

        Assert.assertEquals(kr1.toString(), expected.toString());
    }

    @DataProvider(name = "removes")
    private Object[][] removeCases() throws Exception {
        return new Object[][] {
          { kr("", "0a00"),     krs(kr("0b", null)) },
          { kr("00", "0b"),     krs(kr("0b", null)) },
          { kr("0100", "0b"),   krs(kr("01", "0100"), kr("0b", null)) },
        };
    }

    @Test
    public void testAddRemove() throws Exception {
        final HashSet<KeyRange> expected = new HashSet<>();                             // expected ranges
        final KeyRanges actual = KeyRanges.empty();                                     // actual ranges
        for (int i = 0; i < 100000; i++) {

            // Get bounds
            ByteData minKey;
            ByteData maxKey;
            do {
                final byte[] minKeyBytes = new byte[1 << this.random.nextInt(4)];
                this.random.nextBytes(minKeyBytes);
                final byte[] maxKeyBytes = this.random.nextInt(7) != 0 ? new byte[1 << this.random.nextInt(4)] : null;
                if (maxKeyBytes != null)
                    this.random.nextBytes(maxKeyBytes);
                minKey = ByteData.of(minKeyBytes);
                maxKey = maxKeyBytes != null ? ByteData.of(maxKeyBytes) : null;
            } while (maxKey != null && maxKey.compareTo(minKey) < 0);

            // Create new range
            final KeyRange range = new KeyRange(minKey, maxKey);

            // Mutate
            if (this.random.nextInt(3) < 2) {                                           // add
                actual.add(range);
                expected.add(range);
            } else {                                                                    // remove
                actual.remove(range);
                final KeyRange[] prevs = expected.toArray(new KeyRange[expected.size()]);
                expected.clear();
                for (KeyRange prev : prevs) {
                    if (!range.overlaps(prev)) {
                        expected.add(prev);
                        continue;
                    }
                    if (prev.getMin().compareTo(range.getMin()) < 0)
                        expected.add(new KeyRange(prev.getMin(), range.getMin()));
                    if (range.getMax() != null && (prev.getMax() == null || range.getMax().compareTo(prev.getMax()) < 0))
                        expected.add(new KeyRange(range.getMax(), prev.getMax()));
                }
            }

            // Verify
            Assert.assertEquals(actual.asSet(), new KeyRanges(expected.toArray(new KeyRange[expected.size()])).asSet());
        }
    }

///////////// inverse(), union(), intersection()

    @Test(dataProvider = "KeyRanges")
    public void testKeyRanges(KeyRanges ranges) throws Exception {

        Assert.assertEquals(ranges, ranges.inverse().inverse());
        Assert.assertEquals(ranges.asList(), ranges.inverse().inverse().asList());

        final KeyRanges inverse = ranges.inverse();
        final KeyRanges ranges2 = ranges.clone();
        ranges2.add(inverse);
        Assert.assertEquals(ranges2, KeyRanges.full());
        Assert.assertTrue(ranges2.isFull());

        final KeyRanges ranges3 = ranges.clone();
        ranges3.intersect(inverse);
        Assert.assertEquals(ranges3, KeyRanges.empty());
        Assert.assertTrue(ranges3.isEmpty());

        final ByteData b1 = this.randomBytes(false);
        Assert.assertEquals(ranges.contains(b1), !inverse.contains(b1),
          "ranges " + ranges + " and inverse " + inverse + " agree on containing " + s(b1));

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

    @Test(dataProvider = "intersects")
    public void testIntersects(KeyRanges ranges1, KeyRanges ranges2) throws Exception {
        KeyRanges x = ranges1.clone();
        x.intersect(ranges2.inverse());

        KeyRanges y = ranges1.clone();
        y.remove(ranges2);

        Assert.assertEquals(x, y);
    }

    @DataProvider(name = "intersects")
    private Object[][] dataIntersects() throws Exception {
        final ArrayList<Object[]> paramsList = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            final ArrayList<KeyRanges> paramList = new ArrayList<>(2);
            for (int j = 0; j < 2; j++) {
                final int numRanges = this.random.nextInt(10);
                final ArrayList<KeyRange> rangeList = new ArrayList<>(numRanges);
                for (int k = 0; k < numRanges; k++)
                    rangeList.add(this.randomKeyRange());
                paramList.add(new KeyRanges(rangeList));
            }
            paramsList.add(paramList.toArray());
        }
        return paramsList.toArray(new Object[paramsList.size()][]);
    }

///////////// Empty

    @Test(dataProvider = "empty")
    public void testEmpty(KeyRanges keyRanges) throws Exception {
        Assert.assertEquals(keyRanges.size(), 0, "not empty: " + keyRanges);
        Assert.assertTrue(keyRanges.isEmpty(), "not empty: " + keyRanges);
        Assert.assertEquals(keyRanges, new KeyRanges(), "not empty: " + keyRanges);
    }

    @DataProvider(name = "empty")
    private Object[][] emptyKeyRanges() throws Exception {
        final KeyRange empty1 = new KeyRange(ByteData.empty(), ByteData.empty());
        final KeyRange empty2 = new KeyRange(ByteData.of(33), ByteData.of(33));
        return new KeyRanges[][] {
            { new KeyRanges() },
            { new KeyRanges(empty1) },
            { new KeyRanges(empty2) },
            { new KeyRanges(empty1, empty2) },
            { new KeyRanges(empty2, empty1) },
        };
    }
}
