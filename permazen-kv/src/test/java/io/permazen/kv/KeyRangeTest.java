
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyRangeTest extends KeyRangeTestSupport {

    @Test
    public void testFull() {
        final KeyRange kr = new KeyRange(new byte[0], null);
        Assert.assertTrue(kr.isFull());
    }

    @Test
    public void testPrefix1() {
        final KeyRange kr1 = new KeyRange(new byte[] { 0x20 }, new byte[] { 0x20, 0x60 });
        Assert.assertEquals(kr1.prefixedBy(new byte[0]), kr1);
        final KeyRange kr2 = kr1.prefixedBy(new byte[] { 0x30 });
        Assert.assertEquals(kr2.getMin(), new byte[] { 0x30, 0x20 });
        Assert.assertEquals(kr2.getMax(), new byte[] { 0x30, 0x20, 0x60 });
    }

    @Test
    public void testPrefix2() {
        final KeyRange kr1 = new KeyRange(new byte[] { 0x20 }, null);
        Assert.assertEquals(kr1.prefixedBy(new byte[0]), kr1);
        final KeyRange kr2 = kr1.prefixedBy(new byte[] { 0x30 });
        Assert.assertEquals(kr2.getMin(), new byte[] { 0x30, 0x20 });
        Assert.assertEquals(kr2.getMax(), new byte[] { 0x31 });
    }

    @Test
    public void testPrefix3() {
        final KeyRange kr1 = new KeyRange(new byte[0], null);
        Assert.assertEquals(kr1.prefixedBy(new byte[0]), kr1);
        final KeyRange kr2 = kr1.prefixedBy(new byte[] { 0x30 });
        Assert.assertEquals(kr2.getMin(), new byte[] { 0x30 });
        Assert.assertEquals(kr2.getMax(), new byte[] { 0x31 });
    }

    @Test
    public void testPrefix4() {
        final KeyRange kr1 = new KeyRange(new byte[0], null);
        Assert.assertEquals(kr1.prefixedBy(new byte[0]), kr1);
        final KeyRange kr2 = kr1.prefixedBy(new byte[] { (byte)0xff });
        Assert.assertEquals(kr2.getMin(), new byte[] { (byte)0xff });
        Assert.assertNull(kr2.getMax());
    }

    @Test
    public void testIsSingleKey() {
        final KeyRange kr1 = new KeyRange(new byte[] { 0x01, 0x02 }, new byte[] { 0x01, 0x02, 0x00 });
        Assert.assertTrue(kr1.isSingleKey());
        final KeyRange kr2 = new KeyRange(new byte[] { 0x01, 0x02 }, new byte[] { 0x01, 0x02, 0x00, 0x00 });
        Assert.assertFalse(kr2.isSingleKey());
        final KeyRange kr3 = new KeyRange(new byte[] { 0x01, 0x01 }, new byte[] { 0x01, 0x02 });
        Assert.assertFalse(kr3.isSingleKey());
        final KeyRange kr4 = new KeyRange(new byte[] { 0x01, 0x02 }, new byte[] { 0x02, 0x02, 0x00 });
        Assert.assertFalse(kr4.isSingleKey());
    }
}

