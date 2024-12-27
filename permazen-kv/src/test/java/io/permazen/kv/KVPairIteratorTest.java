
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import io.permazen.kv.util.MemoryKVStore;
import io.permazen.util.ByteData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class KVPairIteratorTest extends KeyRangeTestSupport {

    @Test(dataProvider = "iterations")
    public void testIterations(ByteData[] data, KeyRanges ranges, ByteData[] results) throws Exception {
        if (results == null)
            results = data;

        // Fill KV store with data
        final Function<ByteData, KVPair> pairer = value -> new KVPair(value, value);
        final MemoryKVStore kv = new MemoryKVStore();
        for (ByteData ba : data)
            kv.put(ba, ba);

        // Verify forward iterator matches what we expect
        final KVPairIterator forwardIterator = new KVPairIterator(kv, null, ranges, false);
        final List<KVPair> expectedPairsForward = Lists.transform(Arrays.asList(results), pairer);
        final List<KVPair> actualPairsForward = Lists.newArrayList(forwardIterator);
        Assert.assertEquals(actualPairsForward, expectedPairsForward);

        // Verify reverse iterator matches what we expect
        final ArrayList<ByteData> reversedResults = new ArrayList<>(Arrays.asList(results));
        Collections.reverse(reversedResults);
        final KVPairIterator reverseIterator = new KVPairIterator(kv, null, ranges, true);
        final List<KVPair> expectedPairsReverse = Lists.transform(reversedResults, pairer);
        final List<KVPair> actualPairsReverse = Lists.newArrayList(reverseIterator);
        Assert.assertEquals(actualPairsReverse, expectedPairsReverse);
    }

    @DataProvider(name = "iterations")
    private Object[][] dataIterations() throws Exception {
        return new Object[][] {

            {
                ba("00", "10", "30", "3000", "300000", "40", "50", "60", "70", "80", "99", "ffff"),
                null,
                null,
            },

            {
                ba("00", "10", "30", "3000", "300000", "40", "50", "60", "70", "80", "99", "ffff"),
                KeyRanges.full(),
                null,
            },

            {
                ba("00", "10", "30", "3000", "300000", "40", "50", "60", "70", "80", "99", "ffff"),
                KeyRanges.empty(),
                ba()
            },

            {
                ba("00", "10", "30", "3000", "300000", "40", "50", "60", "70", "80", "99", "ffff"),
                krs(kr(null, "20"), kr("3000", "50"), kr("70", "90"), kr("a0", null)),
                ba("00", "10", "3000", "300000", "40", "70", "80", "ffff")
            },

            {
                ba("00", "10", "30", "3000", "300000", "40", "50", "60", "70", "80", "99", "ffff"),
                krs(kr("3000", "50"), kr("70", "90"), kr("a0", null)),
                ba("3000", "300000", "40", "70", "80", "ffff")
            },

            {
                ba("00", "10", "30", "3000", "300000", "40", "50", "60", "70", "80", "99", "ffff"),
                krs(kr("3000", "50"), kr("70", "90")),
                ba("3000", "300000", "40", "70", "80")
            },

            {
                ba("00", "10", "30", "3000", "300000", "40", "50", "60", "70", "80", "99", "ffff"),
                krs(kr("70", "90")),
                ba("70", "80")
            },

            {
                ba("00", "10", "30", "3000", "300000", "40", "50", "60", "70", "80", "99", "ffff"),
                krs(kr("50", "50")),
                ba()
            },

        };
    }

    @Test
    public void testNextTarget() throws Exception {
        final MemoryKVStore kv = new MemoryKVStore();
        kv.put(b(""), b("33"));
        kv.put(b("0fffff"), b("abcd"));
        kv.put(b("10"), b("aa"));
        kv.put(b("1000"), b("99"));
        kv.put(b("1001"), b("93"));
        kv.put(b("20"), b("2222"));
        kv.put(b("30"), b("3333"));
        kv.put(b("40"), b("4444"));

    // Restrict by KeyFilter

        final KeyRanges keyRanges = krs(kr(null, "1000"), kr("20", "30"), kr("35", "40"), kr("50", null));
        KVPairIterator i = new KVPairIterator(kv, null, keyRanges, false);

        Assert.assertTrue(i.hasNext());
        Assert.assertTrue(i.hasNext());
        Assert.assertEquals(i.next(), new KVPair(b(""), b("33")));

        i.setNextTarget(b(""));
        Assert.assertTrue(i.hasNext());
        Assert.assertEquals(i.next(), new KVPair(b(""), b("33")));

        i.setNextTarget(b("0fffff01"));
        Assert.assertTrue(i.hasNext());
        Assert.assertEquals(i.next(), new KVPair(b("10"), b("aa")));

        i.setNextTarget(b("1001"));
        Assert.assertTrue(i.hasNext());
        Assert.assertEquals(i.next(), new KVPair(b("20"), b("2222")));

        Assert.assertTrue(!i.hasNext());

    // Restrict by KeyRange

        i = new KVPairIterator(kv, b("10"));

        Assert.assertTrue(i.hasNext());
        Assert.assertEquals(i.next(), new KVPair(b("10"), b("aa")));

        Assert.assertTrue(i.hasNext());
        Assert.assertEquals(i.next(), new KVPair(b("1000"), b("99")));

        Assert.assertTrue(i.hasNext());
        Assert.assertEquals(i.next(), new KVPair(b("1001"), b("93")));

        Assert.assertFalse(i.hasNext());

        i.setNextTarget(b(""));
        Assert.assertTrue(i.hasNext());
        Assert.assertEquals(i.next(), new KVPair(b("10"), b("aa")));

        i.setNextTarget(b("30"));
        Assert.assertFalse(i.hasNext());

    // Restrict by both

        i = new KVPairIterator(kv, new KeyRange(b("1001"), b("30")), new KeyRanges(new KeyRange(b("1e"), null)), false);

        Assert.assertTrue(i.hasNext());
        Assert.assertEquals(i.next(), new KVPair(b("20"), b("2222")));
        Assert.assertFalse(i.hasNext());

        i.setNextTarget(b("20"));
        Assert.assertTrue(i.hasNext());

        i.setNextTarget(b("2000"));
        Assert.assertFalse(i.hasNext());

    }
}
