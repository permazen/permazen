
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

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
}

