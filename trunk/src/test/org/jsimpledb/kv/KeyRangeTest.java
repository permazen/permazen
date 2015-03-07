
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
}

