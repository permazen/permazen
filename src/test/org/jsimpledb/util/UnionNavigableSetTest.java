
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
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

public class UnionNavigableSetTest extends TestSupport {

    @Test
    public void testEmpty1() {
        final TreeSet<Integer> t1 = new TreeSet<>();
        t1.add(123);
        t1.add(456);
        t1.add(789);
        final TreeSet<Integer> empty = new TreeSet<>();
        Assert.assertEquals(NavigableSets.union(t1, empty), t1);
    }

    @Test
    public void testEmpty2() {
        Assert.assertEquals(NavigableSets.<Object>union(), NavigableSets.<Object>empty());
    }

    @Test
    public void testRandomUnions() {
        for (int testNum = 0; testNum < 200; testNum++) {

            // Generate sets
            final int numSets = this.random.nextInt(9) + 1;
            final ArrayList<NavigableSet<Integer>> sets = new ArrayList<NavigableSet<Integer>>(numSets);
            for (int i = 0; i < numSets; i++) {
                final TreeSet<Integer> set = new TreeSet<Integer>();
                final int numValues = this.random.nextInt(33);
                for (int j = 0; j < numValues; j++)
                    set.add(this.random.nextInt(100));
                sets.add(set);
            }

            // Verify intersection
            this.verifyUnion(sets);

            // Verify some random intersections
            int minValue = 0;
            int maxValue = 100;
            while (maxValue >= minValue) {

                // Get subsets
                final ArrayList<NavigableSet<Integer>> subSets = new ArrayList<NavigableSet<Integer>>(numSets);
                for (int i = 0; i < numSets; i++)
                    subSets.add(sets.get(i).subSet(minValue, true, maxValue, false));

                // Verify intersection
                this.verifyUnion(subSets);

                // Update bounds
                maxValue -= this.random.nextInt(6);
                minValue += this.random.nextInt(6);
            }
        }
    }

    private void verifyUnion(List<NavigableSet<Integer>> sets) {
        final NavigableSet<Integer> expected = this.calculateUnion(sets);
        final UnionNavigableSet<Integer> actual = new UnionNavigableSet<Integer>(sets);
        Assert.assertEquals(actual, expected);
        Assert.assertEquals(Sets.newTreeSet((Iterable<Integer>)actual), expected);
    }

    private NavigableSet<Integer> calculateUnion(List<NavigableSet<Integer>> sets) {
        final TreeSet<Integer> unionSet = new TreeSet<Integer>();
        for (NavigableSet<Integer> set : sets)
            unionSet.addAll(set);
        return unionSet;
    }
}

