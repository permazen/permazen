
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import io.permazen.test.TestSupport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

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

        // Create one-sided bounds, both upper & lower, both inclusive & exclusive
        final Bounds<Integer> lowerInc = new Bounds<Integer>(lowerBound, BoundType.INCLUSIVE, false);
        final Bounds<Integer> lowerExc = new Bounds<Integer>(lowerBound, BoundType.EXCLUSIVE, false);
        final Bounds<Integer> upperInc = new Bounds<Integer>(upperBound, BoundType.INCLUSIVE, true);
        final Bounds<Integer> upperExc = new Bounds<Integer>(upperBound, BoundType.EXCLUSIVE, true);

        for (int i = lowerBound - 10; i <= upperBound + 10; i++) {
            final int lowerCompare = Integer.compare(i, lowerBound);
            final int upperCompare = Integer.compare(i, upperBound);

            // Compare value to upper & lower bounds
            final boolean expectedInLower = lowerBoundType == BoundType.NONE ? true :
              lowerBoundType == BoundType.INCLUSIVE ? lowerCompare >= 0 : lowerCompare > 0;
            final boolean expectedInUpper = upperBoundType == BoundType.NONE ? true :
              upperBoundType == BoundType.INCLUSIVE ? upperCompare <= 0 : upperCompare < 0;
            Assert.assertEquals(b.isWithinLowerBound(null, i), expectedInLower);
            Assert.assertEquals(b.isWithinUpperBound(null, i), expectedInUpper);

            // Reverse the ordering and do the same thing
            Assert.assertEquals(b.reverse().isWithinLowerBound(Collections.<Integer>reverseOrder(), i), expectedInUpper);
            Assert.assertEquals(b.reverse().isWithinUpperBound(Collections.<Integer>reverseOrder(), i), expectedInLower);

            // Check whether value is within both bounds
            Assert.assertEquals(b.isWithinBounds(null, i), expectedInLower && expectedInUpper);

            // Reverse the ordering and do the same thing
            Assert.assertEquals(b.reverse().isWithinBounds(Collections.<Integer>reverseOrder(), i),
              expectedInLower && expectedInUpper);

            // Check that value is within unbounded bounds
            Assert.assertTrue(b.isWithinBounds(null, new Bounds<Integer>()));

            final boolean expectedLowerInc = lowerCompare >= 0;
            final boolean expectedLowerExc = lowerCompare > 0;
            final boolean expectedUpperInc = upperCompare <= 0;
            final boolean expectedUpperExc = upperCompare < 0;

            // Verify value vs. one-sided bounds
            Assert.assertEquals(lowerInc.isWithinBounds(null, i), expectedLowerInc);
            Assert.assertEquals(lowerExc.isWithinBounds(null, i), expectedLowerExc);
            Assert.assertEquals(upperInc.isWithinBounds(null, i), expectedUpperInc);
            Assert.assertEquals(upperExc.isWithinBounds(null, i), expectedUpperExc);
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

    // CHECKSTYLE OFF: ParenPad
    @Test
    public void testBoundsUnion() {

        final Comparator<Integer> c = Comparator.nullsLast(Integer::compare);

        final Bounds<Integer> b1 = new Bounds<>();

        final Bounds<Integer> b2  = new Bounds<>(-10, BoundType.EXCLUSIVE,  0, BoundType.INCLUSIVE);
        final Bounds<Integer> b3  = new Bounds<>(  0, BoundType.INCLUSIVE, 10, BoundType.INCLUSIVE);
        final Bounds<Integer> b4  = new Bounds<>(-10, BoundType.INCLUSIVE,  0, BoundType.EXCLUSIVE);
        final Bounds<Integer> b5  = new Bounds<>(  0, BoundType.EXCLUSIVE, 10, BoundType.EXCLUSIVE);

        final Bounds<Integer> b6  = new Bounds<>( -5, BoundType.EXCLUSIVE,  5, BoundType.EXCLUSIVE);
        final Bounds<Integer> b7  = new Bounds<>(-50, BoundType.EXCLUSIVE, 50, BoundType.EXCLUSIVE);

        final Bounds<Integer> b8  = new Bounds<>(  0, BoundType.EXCLUSIVE,  0, BoundType.EXCLUSIVE);     // empty instance
        final Bounds<Integer> b9  = new Bounds<>(  0, BoundType.INCLUSIVE,  0, BoundType.EXCLUSIVE);     // empty instance
        final Bounds<Integer> b10 = new Bounds<>(  0, BoundType.INCLUSIVE,  0, BoundType.INCLUSIVE);

        final Bounds<Integer> b11 = new Bounds<>(-50, BoundType.INCLUSIVE, -50, BoundType.INCLUSIVE);
        final Bounds<Integer> b12 = new Bounds<>( 50, BoundType.INCLUSIVE,  50, BoundType.INCLUSIVE);

        Assert.assertFalse(b7.isEmpty(c));
        Assert.assertTrue(b8.isEmpty(c));
        Assert.assertTrue(b9.isEmpty(c));
        Assert.assertFalse(b10.isEmpty(c));

        Assert.assertEquals(b1.union(c, b1), b1);
        Assert.assertEquals(b2.union(c, b2), b2);
        Assert.assertEquals(b3.union(c, b3), b3);
        Assert.assertEquals(b4.union(c, b4), b4);
        Assert.assertEquals(b5.union(c, b5), b5);
        Assert.assertEquals(b6.union(c, b6), b6);
        Assert.assertEquals(b7.union(c, b7), b7);
        Assert.assertEquals(b8.union(c, b8), b8);
        Assert.assertEquals(b9.union(c, b9), b9);

        Assert.assertEquals(b1.union(c, b1), b1);
        Assert.assertEquals(b1.union(c, b2), b1);
        Assert.assertEquals(b1.union(c, b3), b1);
        Assert.assertEquals(b1.union(c, b4), b1);
        Assert.assertEquals(b1.union(c, b5), b1);

        Assert.assertEquals(b2.union(c, b3), new Bounds<>(-10, BoundType.EXCLUSIVE, 10, BoundType.INCLUSIVE));
        Assert.assertEquals(b2.union(c, b4), new Bounds<>(-10, BoundType.INCLUSIVE,  0, BoundType.INCLUSIVE));
        Assert.assertEquals(b3.union(c, b4), new Bounds<>(-10, BoundType.INCLUSIVE, 10, BoundType.INCLUSIVE));
        Assert.assertEquals(b5.union(c, b2), new Bounds<>(-10, BoundType.EXCLUSIVE, 10, BoundType.EXCLUSIVE));

        Assert.assertEquals(b2.union(c, b6), new Bounds<>(-10, BoundType.EXCLUSIVE,  5, BoundType.EXCLUSIVE));
        Assert.assertEquals(b2.union(c, b7), new Bounds<>(-50, BoundType.EXCLUSIVE, 50, BoundType.EXCLUSIVE));

        Assert.assertNull(b4.union(c, b5));

        Assert.assertEquals(b4.union(c, b8), b4);
        Assert.assertEquals(b5.union(c, b8), b5);

        Assert.assertEquals(b4.union(c, b9), b4);
        Assert.assertEquals(b5.union(c, b9), b5);

        Assert.assertEquals(b4.union(c, b10), new Bounds<>(-10, BoundType.INCLUSIVE,  0, BoundType.INCLUSIVE));
        Assert.assertEquals(b5.union(c, b10), new Bounds<>(  0, BoundType.INCLUSIVE, 10, BoundType.EXCLUSIVE));

        Assert.assertEquals(b7.union(c, b11).union(c, b12), new Bounds<>(-50, BoundType.INCLUSIVE, 50, BoundType.INCLUSIVE));

        Assert.assertEquals(b4.union(c, b7), b7);
        Assert.assertEquals(b8.union(c, b6), b6);
        Assert.assertEquals(b9.union(c, b6), b6);
        Assert.assertEquals(b10.union(c, b6), b6);
    }
    // CHECKSTYLE ON: ParenPad
}
