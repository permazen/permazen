
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Converter;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConvertedStuffTest extends TestSupport {

    final Converter<Integer, String> i2s = new IntStringConverter();
    final Converter<String, Integer> s2i = i2s.reverse();

    @Test
    public void testConvertedList() {

        final ArrayList<Integer> ilist = new ArrayList<>();
        ilist.add(123);
        ilist.add(456);
        ilist.add(789);
        ilist.add(456);
        ilist.add(-1);
        Assert.assertEquals(ilist, buildList(123, 456, 789, 456, -1));
        Assert.assertTrue(ilist.contains(789));
        Assert.assertFalse(ilist.contains("789"));
        Assert.assertEquals(ilist.indexOf(456), 1);
        Assert.assertEquals(ilist.lastIndexOf(456), 3);

        final List<String> slist = new ConvertedList<String, Integer>(ilist, this.s2i);
        Assert.assertEquals(slist, buildList("123", "456", "789", "456", "-1"));
        Assert.assertTrue(slist.contains("789"));
        Assert.assertEquals(slist.indexOf("456"), 1);
        Assert.assertEquals(slist.lastIndexOf("456"), 3);

        Assert.assertEquals(slist, new ConvertedList<String, Integer>(new ConvertedList<Integer, String>(slist, i2s), this.s2i));
        Assert.assertEquals(ilist, new ConvertedList<Integer, String>(new ConvertedList<String, Integer>(ilist, s2i), this.i2s));

        slist.add("76");
        slist.remove(1);
        Assert.assertEquals(ilist, buildList(123, 789, 456, -1, 76));
        Assert.assertEquals(slist, buildList("123", "789", "456", "-1", "76"));

        List<String> subList = slist.subList(1, 3);
        Assert.assertEquals(subList, buildList("789", "456"));

        subList.clear();
        Assert.assertEquals(subList, buildList());
        Assert.assertEquals(ilist, buildList(123, -1, 76));
        Assert.assertEquals(slist, buildList("123", "-1", "76"));

        ilist.remove(1);
        ilist.remove((Object)"123");
        slist.remove((Object)123);
        Assert.assertEquals(ilist, buildList(123, 76));
        Assert.assertEquals(slist, buildList("123", "76"));
    }

    @Test
    public void testConvertedSet() {

        final NavigableSet<Integer> iset = new TreeSet<>();
        iset.add(123);
        iset.add(45);
        iset.add(789);
        iset.add(-1);

        final NavigableSet<String> sset = new ConvertedNavigableSet<String, Integer>(iset, this.s2i);
        TestSupport.checkSet(sset, buildSet("123", "45", "789", "-1"));
        Assert.assertTrue(sset.contains("789"));
        Assert.assertFalse(sset.contains(789));
        Assert.assertEquals(Arrays.asList(sset.toArray()), buildList("-1", "45", "123", "789"));

        Assert.assertTrue(sset.comparator().compare("45", "123") < 0);

        Assert.assertEquals(sset, new ConvertedSet<String, Integer>(new ConvertedSet<Integer, String>(sset, this.i2s), this.s2i));
        Assert.assertEquals(iset, new ConvertedSet<Integer, String>(new ConvertedSet<String, Integer>(iset, this.s2i), this.i2s));

        Assert.assertEquals(sset, new ConvertedNavigableSet<String, Integer>(
          new ConvertedNavigableSet<Integer, String>(sset, this.i2s), this.s2i));
        Assert.assertEquals(iset, new ConvertedNavigableSet<Integer, String>(
          new ConvertedNavigableSet<String, Integer>(iset, this.s2i), this.i2s));

        TestSupport.checkSet(sset.tailSet("123"), buildSet("123", "789"));
    }

    @Test
    public void testConvertedMap() {

        final Converter<Object, Object> identity = Converter.<Object>identity();

        final Object obj123 = new Object();
        final Object obj45 = new Object();
        final Object obj789 = new Object();
        final Object objM1 = new Object();

        final NavigableMap<Integer, Object> imap = new TreeMap<>();
        imap.put(123, obj123);
        imap.put(45, obj45);
        imap.put(789, obj789);
        imap.put(-1, objM1);

        final NavigableMap<String, Object> smap
          = new ConvertedNavigableMap<String, Object, Integer, Object>(imap, this.s2i, identity);

        TestSupport.checkMap(smap, buildMap("123", obj123, "45", obj45, "789", obj789, "-1", objM1));
        Assert.assertTrue(smap.keySet().contains("789"));
        Assert.assertTrue(smap.containsKey("789"));
        Assert.assertFalse(smap.keySet().contains(789));
        Assert.assertFalse(smap.containsKey(789));

        Assert.assertTrue(smap.comparator().compare("45", "123") < 0);

        Assert.assertEquals(Arrays.asList(smap.keySet().toArray()), buildList("-1", "45", "123", "789"));
        Assert.assertEquals(Arrays.asList(smap.descendingMap().keySet().toArray()), buildList("789", "123", "45", "-1"));
        Assert.assertEquals(Arrays.asList(smap.values().toArray()), buildList(objM1, obj45, obj123, obj789));
        Assert.assertEquals(Arrays.asList(smap.descendingMap().values().toArray()), buildList(obj789, obj123, obj45, objM1));

        Assert.assertEquals(Arrays.asList(smap.entrySet().toArray()), buildList(
          new AbstractMap.SimpleEntry<String, Object>("-1", objM1),
          new AbstractMap.SimpleEntry<String, Object>("45", obj45),
          new AbstractMap.SimpleEntry<String, Object>("123", obj123),
          new AbstractMap.SimpleEntry<String, Object>("789", obj789)));
        Assert.assertEquals(Arrays.asList(smap.descendingMap().entrySet().toArray()), buildList(
          new AbstractMap.SimpleEntry<String, Object>("789", obj789),
          new AbstractMap.SimpleEntry<String, Object>("123", obj123),
          new AbstractMap.SimpleEntry<String, Object>("45", obj45),
          new AbstractMap.SimpleEntry<String, Object>("-1", objM1)));

        Assert.assertEquals(smap, new ConvertedNavigableMap<String, Object, Integer, Object>(
          new ConvertedNavigableMap<Integer, Object, String, Object>(smap, this.i2s, identity), this.s2i, identity));
        Assert.assertEquals(imap, new ConvertedNavigableMap<Integer, Object, String, Object>(
          new ConvertedNavigableMap<String, Object, Integer, Object>(imap, this.s2i, identity), this.i2s, identity));
    }
}

