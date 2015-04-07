
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

import org.jsimpledb.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyRangeTest extends TestSupport {

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
}

