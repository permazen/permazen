
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import java.util.ArrayList;
import java.util.Collections;

import io.permazen.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BoundsTest extends TestSupport {

    @Test
    public void testBounds1() {

        Assert.assertEquals(new Bounds<Integer>(), new Bounds<Integer>());
        Assert.assertEquals(new Bounds<Integer>(), new Bounds<Integer>(null, BoundType.NONE, null, BoundType.NONE));
        Assert.assertEquals(new Bounds<Integer>(), new Bounds<Integer>(1234, BoundType.NONE, 5678, BoundType.NONE));
        Assert.assertEquals(new Bounds<Integer>(1234, BoundType.INCLUSIVE, false),
          new Bounds<Integer>(1234, BoundType.INCLUSIVE, null, BoundType.NONE));
        Assert.assertEquals(new Bounds<Integer>(1234, BoundType.INCLUSIVE, true),
          new Bounds<Integer>(null, BoundType.NONE, 1234, BoundType.INCLUSIVE));

        Bounds<Integer> b = new Bounds<Integer>(6, BoundType.INCLUSIVE, 7, BoundType.INCLUSIVE);
        Assert.assertEquals(b.isWithinBounds(null, 5), false);
        Assert.assertEquals(b.isWithinBounds(null, 6), true);
        Assert.assertEquals(b.isWithinBounds(null, 7), true);
        Assert.assertEquals(b.isWithinBounds(null, 8), false);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.INCLUSIVE, 7, BoundType.INCLUSIVE)), true);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.INCLUSIVE, 7, BoundType.EXCLUSIVE)), true);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.EXCLUSIVE, 7, BoundType.INCLUSIVE)), true);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.EXCLUSIVE, 7, BoundType.EXCLUSIVE)), true);

        b = new Bounds<Integer>(6, BoundType.INCLUSIVE, 7, BoundType.EXCLUSIVE);
        Assert.assertEquals(b.isWithinBounds(null, 5), false);
        Assert.assertEquals(b.isWithinBounds(null, 6), true);
        Assert.assertEquals(b.isWithinBounds(null, 7), false);
        Assert.assertEquals(b.isWithinBounds(null, 8), false);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.INCLUSIVE, 7, BoundType.INCLUSIVE)), false);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.INCLUSIVE, 7, BoundType.EXCLUSIVE)), true);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.EXCLUSIVE, 7, BoundType.INCLUSIVE)), false);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.EXCLUSIVE, 7, BoundType.EXCLUSIVE)), true);

        b = new Bounds<Integer>(6, BoundType.EXCLUSIVE, 7, BoundType.INCLUSIVE);
        Assert.assertEquals(b.isWithinBounds(null, 5), false);
        Assert.assertEquals(b.isWithinBounds(null, 6), false);
        Assert.assertEquals(b.isWithinBounds(null, 7), true);
        Assert.assertEquals(b.isWithinBounds(null, 8), false);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.INCLUSIVE, 7, BoundType.INCLUSIVE)), false);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.INCLUSIVE, 7, BoundType.EXCLUSIVE)), false);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.EXCLUSIVE, 7, BoundType.INCLUSIVE)), true);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.EXCLUSIVE, 7, BoundType.EXCLUSIVE)), true);

        b = new Bounds<Integer>(6, BoundType.EXCLUSIVE, 7, BoundType.EXCLUSIVE);
        Assert.assertEquals(b.isWithinBounds(null, 5), false);
        Assert.assertEquals(b.isWithinBounds(null, 6), false);
        Assert.assertEquals(b.isWithinBounds(null, 7), false);
        Assert.assertEquals(b.isWithinBounds(null, 8), false);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.INCLUSIVE, 7, BoundType.INCLUSIVE)), false);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.INCLUSIVE, 7, BoundType.EXCLUSIVE)), false);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.EXCLUSIVE, 7, BoundType.INCLUSIVE)), false);
        Assert.assertEquals(b.isWithinBounds(null, new Bounds<Integer>(6, BoundType.EXCLUSIVE, 7, BoundType.EXCLUSIVE)), true);

        Bounds<Integer> b1 = new Bounds<Integer>(5, BoundType.EXCLUSIVE, true);
        Bounds<Integer> b2 = new Bounds<Integer>(10, BoundType.EXCLUSIVE, true);
        Assert.assertFalse(b1.isWithinBounds(null, b2));
    }

    @Test(dataProvider = "cases")
    public void testBounds2(final int lowerBound, final int upperBound,
      final BoundType lowerBoundType, final BoundType upperBoundType) {

        final Bounds<Integer> b = new Bounds<Integer>(lowerBound, lowerBoundType, upperBound, upperBoundType);

        Assert.assertEquals(b.getLowerBound(), lowerBoundType != BoundType.NONE ? lowerBound : null);
        Assert.assertEquals(b.getUpperBound(), upperBoundType != BoundType.NONE ? upperBound : null);
        Assert.assertEquals(b.getLowerBoundType(), lowerBoundType);
        Assert.assertEquals(b.getUpperBoundType(), upperBoundType);

        Assert.assertEquals(b.reverse().reverse(), b);
        Assert.assertEquals(b.reverse().getLowerBound(), upperBoundType != BoundType.NONE ? upperBound : null);
        Assert.assertEquals(b.reverse().getUpperBound(), lowerBoundType != BoundType.NONE ? lowerBound : null);
        Assert.assertEquals(b.reverse().getLowerBoundType(), upperBoundType);
        Assert.assertEquals(b.reverse().getUpperBoundType(), lowerBoundType);

        Assert.assertEquals(b.withoutUpperBound().withUpperBound(upperBound, upperBoundType), b);
        Assert.assertEquals(b.withoutLowerBound().withLowerBound(lowerBound, lowerBoundType), b);

        Assert.assertEquals(b.withoutUpperBound().withoutLowerBound(), new Bounds<Integer>());

        for (int i = lowerBound - 10; i <= upperBound + 10; i++) {
            final int lowerCompare = Integer.compare(i, lowerBound);
            final int upperCompare = Integer.compare(i, upperBound);

            final boolean expectedInLower = lowerBoundType == BoundType.NONE ? true :
              lowerBoundType == BoundType.INCLUSIVE ? lowerCompare >= 0 : lowerCompare > 0;
            final boolean expectedInUpper = upperBoundType == BoundType.NONE ? true :
              upperBoundType == BoundType.INCLUSIVE ? upperCompare <= 0 : upperCompare < 0;
            Assert.assertEquals(b.isWithinLowerBound(null, i), expectedInLower);
            Assert.assertEquals(b.isWithinUpperBound(null, i), expectedInUpper);

            Assert.assertEquals(b.reverse().isWithinLowerBound(Collections.<Integer>reverseOrder(), i), expectedInUpper);
            Assert.assertEquals(b.reverse().isWithinUpperBound(Collections.<Integer>reverseOrder(), i), expectedInLower);

            Assert.assertEquals(b.isWithinBounds(null, i), expectedInLower && expectedInUpper);
            Assert.assertEquals(b.reverse().isWithinBounds(Collections.<Integer>reverseOrder(), i),
              expectedInLower && expectedInUpper);

            Assert.assertTrue(b.isWithinBounds(null, new Bounds<Integer>()));

            final Bounds<Integer> lowerInc = new Bounds<Integer>(i, BoundType.INCLUSIVE, false);
            final Bounds<Integer> lowerExc = new Bounds<Integer>(i, BoundType.EXCLUSIVE, false);
            final Bounds<Integer> upperInc = new Bounds<Integer>(i, BoundType.INCLUSIVE, true);
            final Bounds<Integer> upperExc = new Bounds<Integer>(i, BoundType.EXCLUSIVE, true);

            boolean expectedLowerInc = lowerBoundType == BoundType.NONE ? true :
              lowerCompare < 0 ? false : lowerCompare == 0 ? lowerBoundType == BoundType.INCLUSIVE : true;
            boolean expectedLowerExc = lowerBoundType == BoundType.NONE ? true : lowerCompare < 0 ? false : true;
            boolean expectedUpperInc = upperBoundType == BoundType.NONE ? true :
              upperCompare > 0 ? false : upperCompare == 0 ? upperBoundType == BoundType.INCLUSIVE : false;
            boolean expectedUpperExc = upperBoundType == BoundType.NONE ? true : upperCompare > 0 ? false : true;
        }
    }

    @DataProvider(name = "cases")
    public Object[][] genCases() {
        final ArrayList<Object[]> list = new ArrayList<>();
        final int upperBound = 17;
        for (int lowerBound = 12; lowerBound <= upperBound; lowerBound++) {
            for (BoundType lowerBoundType : BoundType.values()) {
                for (BoundType upperBoundType : BoundType.values())
                    list.add(new Object[] { lowerBound, upperBound, lowerBoundType, upperBoundType });
            }
        }
        return list.toArray(new Object[list.size()][]);
    }
}

