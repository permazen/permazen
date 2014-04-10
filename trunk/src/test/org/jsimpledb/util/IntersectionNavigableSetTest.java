
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

