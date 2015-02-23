
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jsimpledb.TestSupport;
import org.jsimpledb.core.ObjId;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ObjIdMapTest extends TestSupport {

    @Test
    public void testObjIdMap() throws Exception {

        final ObjIdMap<Integer> actual = new ObjIdMap<>();
        final HashMap<ObjId, Integer> expected = new HashMap<>();

        for (int i = 0; i < 5000; i++) {
            int shift = 0;
            while (shift < 31 && this.random.nextBoolean())
                shift++;
            final ObjId id = new ObjId(0x0100000000000000L | this.random.nextInt(3 << shift));
            final int action = this.random.nextInt(100);
            final Integer value = new Integer(this.random.nextInt(4));
            boolean expectedResult = false;
            boolean actualResult = false;
            if (action < 3) {
                actual.clear();
                expected.clear();
            } else if (action < 6) {
                final int pos = this.random.nextInt(10);
                final Iterator<ObjId> iter = actual.keySet().iterator();
                final ObjId id2 = Iterators.get(iter, pos, null);
                if (id2 != null) {
                    iter.remove();
                    expected.remove(id2);
                }
            } else if (action < 40) {
                actualResult = !actual.entrySet().equals(expected.entrySet());
            } else if (action < 45) {
                actualResult = value.equals(actual.put(id, value));
                expectedResult = value.equals(expected.put(id, value));
            } else if (action < 85) {
                actualResult = value.equals(actual.remove(id));
                expectedResult = value.equals(expected.remove(id));
            } else if (action < 90) {
                actualResult = actual.containsValue(value);
                expectedResult = expected.containsValue(value);
            } else {
                actualResult = actual.containsKey(id);
                expectedResult = expected.containsKey(id);
            }
            TestSupport.checkMap(actual, expected);
            Assert.assertEquals(actualResult, expectedResult,
              "wrong result: actual=" + actual.debugDump() + " expected=" + expected);
        }
    }

    @Test
    public void testObjIdMap2() throws Exception {

        final ObjIdMap<String> map = new ObjIdMap<>();

        final ObjId id1 = new ObjId(10);
        final ObjId id2 = new ObjId(10);
        final ObjId id3 = new ObjId(10);

        map.put(id1, "aaa");
        map.put(id2, "bbb");
        map.put(id3, "ccc");

        final HashMap<ObjId, String> hmap = new HashMap<>();

        hmap.put(id1, "aaa");
        hmap.put(id2, "bbb");
        hmap.put(id3, "ccc");

        TestSupport.checkMap(map, hmap);

        Assert.assertEquals(map, hmap);
        Assert.assertEquals(hmap, map);

        map.putAll(hmap);

        TestSupport.checkMap(map, hmap);

        final ObjIdMap<String> copy = map.clone();

        TestSupport.checkMap(copy, map);

        for (Iterator<Map.Entry<ObjId, String>> i = map.entrySet().iterator(); i.hasNext(); ) {
            final Map.Entry<ObjId, String> entry = i.next();
            if (entry.getKey().equals(id1)) {
                i.remove();
                break;
            }
        }

        Assert.assertEquals(map.keySet(), Sets.newHashSet(id2, id3));
        Assert.assertEquals(Sets.newHashSet(map.values()), Sets.newHashSet("bbb", "ccc"));

        map.putAll(copy);

        Assert.assertEquals(map.keySet(), Sets.newHashSet(id1, id2, id3));
        Assert.assertEquals(Sets.newHashSet(map.values()), Sets.newHashSet("aaa", "bbb", "ccc"));
        Assert.assertEquals(map.get(id3), "ccc");

        map.entrySet().remove(new AbstractMap.SimpleEntry<ObjId, String>(id2, "foo"));

        Assert.assertEquals(map, copy);

        map.entrySet().remove(new AbstractMap.SimpleEntry<ObjId, String>(id2, "bbb"));

        TestSupport.checkSet(map.keySet(), Sets.newHashSet(id1, id3));

        // Test null key
        try {
            map.put(null, "blah");
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Test null value
        map.put(id2, null);
        hmap.put(id2, null);

        TestSupport.checkMap(map, hmap);
    }
}

