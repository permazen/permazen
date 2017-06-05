
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ObjIdTest extends CoreAPITestSupport {

    @Test
    public void testSentinel() throws Exception {

        ObjId id1 = ObjId.getSentinel(7);
        Assert.assertEquals(id1.asLong(), 0x0700000000000000L);
        Assert.assertTrue(id1.isSentinel());

        ObjId id2 = new ObjId(7);
        Assert.assertNotEquals(id2.asLong(), 0x0700000000000000L);
        Assert.assertFalse(id2.isSentinel());

        ObjId id3 = ObjId.getSentinel(0x765432fb);
        Assert.assertEquals(id3.asLong(), 0xfe76543200000000L);
        Assert.assertTrue(id3.isSentinel());

        ObjId id4 = new ObjId(0x765432fb);
        Assert.assertNotEquals(id4.asLong(), 0xfe76543200000000L);
        Assert.assertFalse(id4.isSentinel());
    }
}
