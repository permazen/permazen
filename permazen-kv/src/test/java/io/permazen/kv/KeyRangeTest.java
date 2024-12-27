
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import io.permazen.util.ByteData;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyRangeTest extends KeyRangeTestSupport {

    @Test
    public void testFull() {
        final KeyRange kr = new KeyRange(ByteData.empty(), null);
        Assert.assertTrue(kr.isFull());
    }

    @Test
    public void testPrefix1() {
        final KeyRange kr1 = new KeyRange(ByteData.of(0x20), ByteData.of(0x20, 0x60));
        Assert.assertEquals(kr1.prefixedBy(ByteData.empty()), kr1);
        final KeyRange kr2 = kr1.prefixedBy(ByteData.of(0x30));
        Assert.assertEquals(kr2.getMin(), ByteData.of(0x30, 0x20));
        Assert.assertEquals(kr2.getMax(), ByteData.of(0x30, 0x20, 0x60));
    }

    @Test
    public void testPrefix2() {
        final KeyRange kr1 = new KeyRange(ByteData.of(0x20), null);
        Assert.assertEquals(kr1.prefixedBy(ByteData.empty()), kr1);
        final KeyRange kr2 = kr1.prefixedBy(ByteData.of(0x30));
        Assert.assertEquals(kr2.getMin(), ByteData.of(0x30, 0x20));
        Assert.assertEquals(kr2.getMax(), ByteData.of(0x31));
    }

    @Test
    public void testPrefix3() {
        final KeyRange kr1 = new KeyRange(ByteData.empty(), null);
        Assert.assertEquals(kr1.prefixedBy(ByteData.empty()), kr1);
        final KeyRange kr2 = kr1.prefixedBy(ByteData.of(0x30));
        Assert.assertEquals(kr2.getMin(), ByteData.of(0x30));
        Assert.assertEquals(kr2.getMax(), ByteData.of(0x31));
    }

    @Test
    public void testPrefix4() {
        final KeyRange kr1 = new KeyRange(ByteData.empty(), null);
        Assert.assertEquals(kr1.prefixedBy(ByteData.empty()), kr1);
        final KeyRange kr2 = kr1.prefixedBy(ByteData.of(0xff));
        Assert.assertEquals(kr2.getMin(), ByteData.of(0xff));
        Assert.assertNull(kr2.getMax());
    }

    @Test
    public void testIsSingleKey() {
        final KeyRange kr1 = new KeyRange(ByteData.of(0x01, 0x02), ByteData.of(0x01, 0x02, 0x00));
        Assert.assertTrue(kr1.isSingleKey());
        final KeyRange kr2 = new KeyRange(ByteData.of(0x01, 0x02), ByteData.of(0x01, 0x02, 0x00, 0x00));
        Assert.assertFalse(kr2.isSingleKey());
        final KeyRange kr3 = new KeyRange(ByteData.of(0x01, 0x01), ByteData.of(0x01, 0x02));
        Assert.assertFalse(kr3.isSingleKey());
        final KeyRange kr4 = new KeyRange(ByteData.of(0x01, 0x02), ByteData.of(0x02, 0x02, 0x00));
        Assert.assertFalse(kr4.isSingleKey());
    }
}
