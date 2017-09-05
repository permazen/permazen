
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import io.permazen.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ImmutableStuffTest extends TestSupport {

    @Test
    public void testImmutableSet() {

        final NavigableSet<Integer> source = new TreeSet<>();
        source.add(123);
        source.add(45);
        source.add(789);
        source.add(-1);

        final ImmutableNavigableSet<Integer> set = new ImmutableNavigableSet<>(source);

        TestSupport.checkSet(set, buildSet(123, 45, 789, -1));

        Assert.assertTrue(set.contains(789));
        Assert.assertEquals(Arrays.asList(set.toArray()), buildList(-1, 45, 123, 789));

        Assert.assertEquals((int)set.first(), -1);
        Assert.assertEquals((int)set.last(), 789);
        Assert.assertEquals((int)set.ceiling(45), 45);
        Assert.assertEquals((int)set.higher(45), 123);
        Assert.assertEquals((int)set.floor(123), 123);
        Assert.assertEquals((int)set.lower(123), 45);

        TestSupport.checkSet(set.headSet(123, false), buildSet(-1, 45));
        TestSupport.checkSet(set.headSet(123, true), buildSet(-1, 45, 123));

        TestSupport.checkSet(set.tailSet(123, false), buildSet(789));
        TestSupport.checkSet(set.tailSet(123, true), buildSet(123, 789));

        Assert.assertEquals(set.subSet(50, 1000), buildList(123, 789));
        Assert.assertEquals(set.subSet(-1000, 50), buildList(-1, 45));
    }

    @Test
    public void testImmutableMap() {

        final Object obj123 = new Object();
        final Object obj45 = new Object();
        final Object obj789 = new Object();
        final Object objM1 = new Object();

        final NavigableMap<Integer, Object> source = new TreeMap<>();
        source.put(123, obj123);
        source.put(45, obj45);
        source.put(789, obj789);
        source.put(-1, objM1);

        final ImmutableNavigableMap<Integer, Object> map = new ImmutableNavigableMap<>(source);

        TestSupport.checkMap(map, buildMap(123, obj123, 45, obj45, 789, obj789, -1, objM1));

        Assert.assertEquals((int)map.firstKey(), -1);
        Assert.assertEquals((int)map.lastKey(), 789);
        Assert.assertEquals((int)map.ceilingKey(45), 45);
        Assert.assertEquals((int)map.higherKey(45), 123);
        Assert.assertEquals((int)map.floorKey(123), 123);
        Assert.assertEquals((int)map.lowerKey(123), 45);

        Assert.assertTrue(map.keySet().contains(789));
        Assert.assertTrue(map.containsKey(789));
        Assert.assertFalse(map.keySet().contains(333));
        Assert.assertFalse(map.containsKey(333));

        Assert.assertEquals(Arrays.asList(map.keySet().toArray()), buildList(-1, 45, 123, 789));
        Assert.assertEquals(Arrays.asList(map.descendingMap().keySet().toArray()), buildList(789, 123, 45, -1));
        Assert.assertEquals(Arrays.asList(map.values().toArray()), buildList(objM1, obj45, obj123, obj789));
        Assert.assertEquals(Arrays.asList(map.descendingMap().values().toArray()), buildList(obj789, obj123, obj45, objM1));

        Assert.assertEquals(Arrays.asList(map.entrySet().toArray()), buildList(
          new AbstractMap.SimpleEntry<>(-1, objM1),
          new AbstractMap.SimpleEntry<>(45, obj45),
          new AbstractMap.SimpleEntry<>(123, obj123),
          new AbstractMap.SimpleEntry<>(789, obj789)));
        Assert.assertEquals(Arrays.asList(map.descendingMap().entrySet().toArray()), buildList(
          new AbstractMap.SimpleEntry<>(789, obj789),
          new AbstractMap.SimpleEntry<>(123, obj123),
          new AbstractMap.SimpleEntry<>(45, obj45),
          new AbstractMap.SimpleEntry<>(-1, objM1)));
    }
}

