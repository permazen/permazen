
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import io.permazen.test.TestSupport;
import io.permazen.util.Bounds;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.ConvertedNavigableMap;
import io.permazen.util.ConvertedNavigableSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;

import org.testng.annotations.Test;

public class KVNavigableMapTest extends TestSupport {

    @Test
    public void testKVNavigableMap() throws Exception {

        // Create reference map
        final TreeMap<ByteData, ByteData> map1 = new TreeMap<>();

        // Create k/v map
        final MemoryKVStore kv2 = new MemoryKVStore();
        final KVNavigableMap map2 = new KVNavigableMap(kv2);

        // Verify they behave the same
        for (int i = 0; i < 250; i++) {
            final ByteData[] keys = this.randKeys(4);
            final Consumer<NavigableMap<ByteData, ByteData>> op;
            switch (this.random.nextInt(4)) {
            case 0:
                op = map -> map.put(keys[0], keys[1]);
                break;
            case 1:
                op = map -> map.remove(keys[2]);
                break;
            case 2:
                op = map -> map.get(keys[3]);
                break;
            case 3:
                op = this.random.nextInt(20) == 0 ? Map::clear : map -> { };
                break;
            default:
                throw new RuntimeException();
            }

            final TreeMap<ByteData, ByteData> snap1 = new TreeMap<>(map1);
            final TreeMap<ByteData, ByteData> snap2 = new TreeMap<>(map2);
            try {
                this.verifySame(0, map1, map2);
            } catch (AssertionError e) {
                this.log.error("TEST FAILED ON:\nmap1 = {}\nbounds = {}\nmap2 = {}",
                  stringView(snap1), this.toString(map2.getBounds()), stringView(snap2));
                throw e;
            }
        }
    }

    public String toString(Bounds<ByteData> bounds) {
        final String lowerBound = ByteUtil.STRING_CONVERTER.convert(bounds.getLowerBound());
        final String upperBound = ByteUtil.STRING_CONVERTER.convert(bounds.getUpperBound());
        return "" + new Bounds<String>(lowerBound, bounds.getLowerBoundType(), upperBound, bounds.getUpperBoundType());
    }

    public void verifySame(int depth, NavigableMap<ByteData, ByteData> map1, NavigableMap<ByteData, ByteData> map2) {
        if (++depth >= 5)
            return;
        //this.log.info("START verifySame() depth={}", depth);
        final ByteData[] keys = this.randKeys(25);
        final boolean[] boos = new boolean[] {
            this.random.nextBoolean(),
            this.random.nextBoolean(),
            this.random.nextBoolean(),
            this.random.nextBoolean()
        };

        // NavigableMap
        this.checkSameResult(depth, map1, map2, map -> map,
          "map");
        this.checkSameResult(depth, map1, map2, map -> map.ceilingEntry(keys[0]),
          "map.ceilingEntry(%s)", keys[0]);
        this.checkSameResult(depth, map1, map2, map -> map.ceilingKey(keys[1]),
          "map.ceilingKey(%s)", keys[1]);
        this.checkSameResult(depth, map1, map2, NavigableMap::descendingKeySet,
          "map.descendingKeySet()");
        this.checkSameResult(depth, map1, map2, NavigableMap::descendingMap,
          "map.descendingMap()");
        this.checkSameResult(depth, map1, map2, NavigableMap::firstEntry,
          "map.firstEntry()");
        this.checkSameResult(depth, map1, map2, map -> map.floorEntry(keys[2]),
          "map.floorEntry(%s)", keys[2]);
        this.checkSameResult(depth, map1, map2, map -> map.floorKey(keys[3]),
          "map.floorKey(%s)", keys[3]);
        this.checkSameResult(depth, map1, map2, map -> map.headMap(keys[4]),
          "map.headMap(%s)", keys[4]);
        this.checkSameResult(depth, map1, map2, map -> map.headMap(keys[5], boos[0]),
          "map.headMap(%s, %s)", keys[5], boos[0]);
        this.checkSameResult(depth, map1, map2, map -> map.higherEntry(keys[6]),
          "map.higherEntry(%s)", keys[6]);
        this.checkSameResult(depth, map1, map2, map -> map.higherKey(keys[7]),
          "map.higherKey(%s)", keys[7]);
        this.checkSameResult(depth, map1, map2, NavigableMap::lastEntry,
          "map.lastEntry()");
        this.checkSameResult(depth, map1, map2, map -> map.lowerKey(keys[8]),
          "map.lowerKey(%s)", keys[8]);
        this.checkSameResult(depth, map1, map2, NavigableMap::navigableKeySet,
          "map.navigableKeySet()");
        this.checkSameResult(depth, map1, map2, NavigableMap::pollFirstEntry,
          "map.pollFirstEntry()");
        this.checkSameResult(depth, map1, map2, NavigableMap::pollLastEntry,
          "map.pollLastEntry()");
        this.checkSameResult(depth, map1, map2, map -> map.subMap(keys[9], boos[1], keys[10], boos[2]),
          "map.subMap(%s, %s, %s, %s)", keys[9], boos[1], keys[10], boos[2]);
        this.checkSameResult(depth, map1, map2, map -> map.subMap(keys[11], keys[12]),
          "map.subMap(%s, %s)", keys[11], keys[12]);
        this.checkSameResult(depth, map1, map2, map -> map.headMap(keys[13]),
          "map.headMap(%s)", keys[13]);
        this.checkSameResult(depth, map1, map2, map -> map.headMap(keys[14], boos[3]),
          "map.headMap(%s, %s)", keys[14], boos[3]);

        // SortedMap
        this.checkSameResult(depth, map1, map2, NavigableMap::comparator,
          "map.comparator()");
        this.checkSameResult(depth, map1, map2, NavigableMap::firstKey,
          "map.firstKey()");
        this.checkSameResult(depth, map1, map2, NavigableMap::lastKey,
          "map.lastKey()");

        // Map
        this.checkSameResult(depth, map1, map2, map -> map.get(keys[15]),
          "map.get(%s)", keys[15]);
        this.checkSameResult(depth, map1, map2, Map::size,
          "map.size()");
        this.checkSameResult(depth, map1, map2, Map::values,
          "map.values()");
        this.checkSameResult(depth, map1, map2, Map::entrySet,
          "map.entrySet()");
        this.checkSameResult(depth, map1, map2, Map::keySet,
          "map.keySet()");
        this.checkSameResult(depth, map1, map2, Map::isEmpty,
          "map.isEmpty()");

        //this.log.info("FINISH verifySame() depth={}", depth);
    }

    private ByteData randKey() {
        final int len = this.random.nextInt(3);
        final byte[] r = new byte[len];
        this.random.nextBytes(r);
        return ByteData.of(r);
    }

    private ByteData[] randKeys(int max) {
        final ByteData[] r = new ByteData[max];
        for (int i = 0; i < r.length; i++)
            r[i] = this.randKey();
        return r;
    }

    @SuppressWarnings("unchecked")
    private void checkSameResult(int depth,
      NavigableMap<ByteData, ByteData> map1,
      NavigableMap<ByteData, ByteData> map2,
      Function<NavigableMap<ByteData, ByteData>, Object> func, String format, Object... params) {

        // Format description
        for (int i = 0; i < params.length; i++) {
            if (params[i] instanceof ByteData)
                params[i] = ((ByteData)params[i]).toHex();
        }
        final String desc = String.format(format, params);

        // Invoke function
        Exception e1 = null;
        Exception e2 = null;
        Object o1 = null;
        Object o2 = null;
        try {
            //this.log.debug("apply to map1: map -> {}", desc);
            o1 = func.apply(map1);
            //this.log.debug("result from map1: {}", o1);
        } catch (Exception e) {
            //this.log.debug("error from map1: {}", e.toString());
            e1 = e;
        }
        try {
            //this.log.debug("apply to map2: map -> {}", desc);
            o2 = func.apply(map2);
            //this.log.debug("result from map2: {}", o2);
        } catch (Exception e) {
            //this.log.debug("error from map2: {}", e.toString());
            e2 = e;
        }

        // Compare exceptions
        if (e1 != null || e2 != null) {
            if (e1 == null || e2 == null) {
                final Exception e = e1 != null ? e1 : e2;

                //
                // Ignore inconsistent behavior in TreeMap:
                //
                // jshell> var m = new TreeMap<String, String>()
                // m ==> {}
                //
                // jshell> var m2 = m.subMap("a", true, "a", false)
                // m2 ==> {}
                //
                // jshell> var m3 = m2.subMap("a", true, "a", false)
                // |  Exception java.lang.IllegalArgumentException: fromKey out of range
                // |        at TreeMap$AscendingSubMap.subMap (TreeMap.java:2179)
                // |        at do_it$Aux (#18:1)
                // |        at (#18:1)
                //
                if (e instanceof IllegalArgumentException) {
                    //this.log.debug("exception mismatch: [{}] vs. [{}]", e1, e2);
                    return;
                }
                this.log.warn("NON NULL EXCEPTION", e);
                assert false : "exception mismatch: [" + e1 + "] vs. [" + e2 + "]";
            }
            assert e1 != null && e2 != null : "exception mismatch: " + e1 + " vs. " + e2;
            Class<?> t1 = e1.getClass();
            Class<?> t2 = e2.getClass();
            while (t1.getSuperclass() != Exception.class)
                t1 = t1.getSuperclass();
            while (t2.getSuperclass() != Exception.class)
                t2 = t2.getSuperclass();
            assert t1 == t2 : "exception type mismatch: " + t1 + " vs. " + t2;
            return;
        }

        // Compare values
        this.checkSameThing(o1, o2);

        // Recurse on sub-maps
        if (o1 instanceof NavigableMap && (o1 != map1 || o2 != map2))
            this.verifySame(depth, (NavigableMap<ByteData, ByteData>)o1, (NavigableMap<ByteData, ByteData>)o2);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void checkSameThing(Object o1, Object o2) {
        if (Objects.equals(o1, o2))
            return;
        if (o1 == null || o2 == null)
            throw new AssertionError("different nullness, non-null = " + (o1 != null ? o2 : o2));
        if (o1 instanceof Map && o2 instanceof Map) {
            try {
                this.checkSameThing(((Map)o1).entrySet(), ((Map)o2).entrySet());
            } catch (AssertionError e) {
                throw new AssertionError("maps have different entry sets: " + e.getMessage(), e);
            }
            return;
        }
        if (o1 instanceof Map.Entry && o2 instanceof Map.Entry) {
            try {
                this.checkSameThing(((Map.Entry)o1).getKey(), ((Map.Entry)o2).getKey());
            } catch (AssertionError e) {
                throw new AssertionError("map entries have different keys: " + e.getMessage(), e);
            }
            try {
                this.checkSameThing(((Map.Entry)o1).getValue(), ((Map.Entry)o2).getValue());
            } catch (AssertionError e) {
                throw new AssertionError("map entries have different values: " + e.getMessage(), e);
            }
            return;
        }
        if (o1 instanceof ByteData && o2 instanceof ByteData) {
            if (!Objects.equals(o1, o2)) {
                throw new AssertionError("different byte[] arrays: " + ((ByteData)o1).toHex() + " vs. " + ((ByteData)o2).toHex());
            }
            return;
        }
        if (o1 instanceof Collection && o2 instanceof Collection) {
            final int size1 = ((Collection)o1).size();
            final int size2 = ((Collection)o2).size();
            if (size1 != size2)
                throw new AssertionError("sets have different sizes " + size1 + " != " + size2);
            final Iterator<?> i1 = ((Collection)o1).iterator();
            final Iterator<?> i2 = ((Collection)o2).iterator();
            for (int i = 0; true; i++) {
                final boolean hasNext1 = i1.hasNext();
                final boolean hasNext2 = i2.hasNext();
                if (!hasNext1 && !hasNext2) {
                    if (i != size1)
                        throw new AssertionError("iterators terminated at index " + i + " != " + size1);
                    return;
                }
                if (!hasNext1 || !hasNext2)
                    throw new AssertionError("iterators terminate differently at index " + i);
                try {
                    this.checkSameThing(i1.next(), i2.next());
                } catch (AssertionError e) {
                    throw new AssertionError("sets differ at index " + i + ": " + e.getMessage(), e);
                }
            }
        }
        throw new AssertionError("non-equal with unrecognized type(s): "
          + o1 + " (a " + o1.getClass() + ") vs. "
          + o2 + " (a " + o2.getClass() + ")");
    }

    public static NavigableMap<String, String> stringView(NavigableMap<ByteData, ByteData> byteMap) {
        if (byteMap == null)
            return null;
        return new ConvertedNavigableMap<String, String, ByteData, ByteData>(byteMap,
          ByteUtil.STRING_CONVERTER.reverse(), ByteUtil.STRING_CONVERTER.reverse());
    }

    public static NavigableSet<String> stringView(NavigableSet<ByteData> byteSet) {
        if (byteSet == null)
            return null;
        return new ConvertedNavigableSet<String, ByteData>(byteSet, ByteUtil.STRING_CONVERTER.reverse());
    }
}
