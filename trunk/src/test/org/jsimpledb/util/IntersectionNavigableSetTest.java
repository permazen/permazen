
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.jsimpledb.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IntersectionNavigableSetTest extends TestSupport {

    @Test
    public void testEmpty1() {
        final TreeSet<Integer> t1 = new TreeSet<>();
        t1.add(123);
        t1.add(456);
        t1.add(789);
        final TreeSet<Integer> empty = new TreeSet<>();
        Assert.assertEquals(NavigableSets.intersection(t1, empty), NavigableSets.empty());
    }

    @Test
    public void testEmpty2() {
        Assert.assertEquals(NavigableSets.<Object>intersection(), NavigableSets.empty());
    }

    @Test
    public void testRandomIntersections() {
        for (int testNum = 0; testNum < 200; testNum++) {

            // Generate sets
            final int numSets = this.random.nextInt(9) + 1;
            final ArrayList<NavigableSet<Integer>> sets = new ArrayList<NavigableSet<Integer>>(numSets);
            for (int i = 0; i < numSets; i++) {
                NavigableSet<Integer> set = new TreeSet<Integer>();
                final int numValues = this.random.nextInt(66);
                for (int j = 0; j < numValues; j++)
                    set.add(this.random.nextInt(100));
                switch (this.random.nextInt(10)) {
                case 0:
                    set = set.tailSet(this.random.nextInt(200) - 100, this.random.nextBoolean());
                    break;
                case 1:
                    set = set.headSet(this.random.nextInt(200) - 100, this.random.nextBoolean());
                    break;
                case 2:
                    int max = this.random.nextInt(200);
                    int min = this.random.nextInt(max + 1);
                    set = set.subSet(min - 100, this.random.nextBoolean(), max - 100, this.random.nextBoolean());
                    break;
                default:
                    break;
                }
                sets.add(set);
            }

            // Verify intersection
            this.verifyIntersection(sets);

            // Verify some random intersections
            int minValue = 0;
            int maxValue = 100;
            while (maxValue >= minValue) {

                // Get subsets
                final ArrayList<NavigableSet<Integer>> subSets = new ArrayList<NavigableSet<Integer>>(numSets);
                for (int i = 0; i < numSets; i++) {
                    try {
                        subSets.add(sets.get(i).subSet(minValue, true, maxValue, false));
                    } catch (IllegalArgumentException e) {
                        // ignore
                    }
                }

                // Verify intersection
                if (!subSets.isEmpty())
                    this.verifyIntersection(subSets);

                // Update bounds
                maxValue -= this.random.nextInt(6);
                minValue += this.random.nextInt(6);
            }
        }
    }

    @Test
    public void testSubSet() {

        NavigableSet<Integer> set1 = new TreeSet<>();
        set1.add(12);
        set1.add(13);
        set1.add(16);
        set1.add(18);
        set1.add(20);
        set1.add(21);
        NavigableSet<Integer> set2 = new TreeSet<>();
        set2.add(12);
        set2.add(14);
        set2.add(16);
        set2.add(17);
        set2.add(18);
        set2.add(21);
        set2.add(23);
        set2.add(27);

        final NavigableSet<Integer> intersect = NavigableSets.intersection(set1, set2);
        Assert.assertEquals(intersect, buildSet(12, 16, 18, 21));

        Assert.assertEquals(intersect.headSet(18, true), buildSet(12, 16, 18));
        Assert.assertEquals(intersect.headSet(18, false), buildSet(12, 16));
        Assert.assertEquals(intersect.tailSet(16, true), buildSet(16, 18, 21));
        Assert.assertEquals(intersect.tailSet(16, false), buildSet(18, 21));
        Assert.assertEquals(intersect.subSet(16, true, 21, false), buildSet(16, 18));
        Assert.assertEquals(intersect.subSet(16, false, 21, true), buildSet(18, 21));

        NavigableSet<Integer> i = NavigableSets.intersection(set1, set2);
        Assert.assertEquals(i.subSet(15, true, 25, true), buildSet(16, 18, 21));

        set1 = set1.subSet(15, true, 21, false);        // 16, 18, 20
        set2 = set2.subSet(16, true, 23, true);         // 16, 17, 18, 21, 23

        final NavigableSet<Integer> intersect2 = NavigableSets.intersection(set1, set2);
        Assert.assertEquals(intersect2, buildSet(16, 18));

        Assert.assertEquals(intersect2.headSet(18, true), buildSet(16, 18));
        Assert.assertEquals(intersect2.headSet(18, false), buildSet(16));
        Assert.assertEquals(intersect2.tailSet(16, true), buildSet(16, 18));
        Assert.assertEquals(intersect2.tailSet(16, false), buildSet(18));
        Assert.assertEquals(intersect2.subSet(16, true, 21, false), buildSet(16, 18));
        Assert.assertEquals(intersect2.subSet(16, false, 21, true), buildSet(18));

        NavigableSet<Integer> set3 = new TreeSet<>();
        set3.add(100);
        set3.add(200);
        set3 = set3.subSet(100, true, 250, false);
        NavigableSet<Integer> set4 = new TreeSet<>();
        set4.add(100);
        set4.add(200);
        set4.add(300);
        set4.add(400);
        set4 = set4.subSet(100, true, 450, false);

        final NavigableSet<Integer> intersect3 = NavigableSets.intersection(set3, set4);
        Assert.assertEquals(intersect3, buildSet(100, 200));

        final NavigableSet<Integer> intersect4 = NavigableSets.intersection(set4, set3);
        Assert.assertEquals(intersect4, buildSet(100, 200));
    }

    private void verifyIntersection(List<NavigableSet<Integer>> sets) {
        final NavigableSet<Integer> expected = this.calculateIntersection(sets);
        final IntersectionNavigableSet<Integer> actual = new IntersectionNavigableSet<Integer>(sets);
        Assert.assertEquals(actual, expected);
        Assert.assertEquals(Sets.newTreeSet((Iterable<Integer>)actual), expected);
    }

    private NavigableSet<Integer> calculateIntersection(List<NavigableSet<Integer>> sets) {
        final TreeSet<Integer> set = new TreeSet<Integer>();
        switch (sets.size()) {
        case 0:
            return new TreeSet<Integer>();
        case 1:
            return sets.get(0);
        default:
            break;
        }
        for (Integer i : sets.get(0)) {
            boolean foundInEverySet = true;
            for (int j = 1; j < sets.size(); j++) {
                if (!sets.get(j).contains(i)) {
                    foundInEverySet = false;
                    break;
                }
            }
            if (foundInEverySet)
                set.add(i);
        }
        return set;
    }
}

