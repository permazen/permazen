
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import io.permazen.test.TestSupport;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TimestampTest extends TestSupport {

    @Test(dataProvider = "compare")
    public void testTimestamps(Timestamp t1, Timestamp t2, Integer expected) throws Exception {
        final int actual;
        try {
            actual = t1.compareTo(t2);
            assert expected != null : "expected exception but got " + actual;
            Assert.assertEquals(actual, (int)expected);
        } catch (IllegalArgumentException e) {
            assert expected == null : "expected " + expected + " but got " + e;
        }
    }

    @DataProvider(name = "compare")
    private Object[][] messages() {
        final Timestamp now = new Timestamp();
        return new Object[][] {
            { now, now, 0 },

            { new Timestamp(0x00000000), new Timestamp(0x00000000), 0 },
            { new Timestamp(0x00000001), new Timestamp(0x00000001), 0 },
            { new Timestamp(0x7fffffff), new Timestamp(0x7fffffff), 0 },
            { new Timestamp(0x80000000), new Timestamp(0x80000000), 0 },
            { new Timestamp(0xffffffff), new Timestamp(0xffffffff), 0 },

            { new Timestamp(0x00000000), new Timestamp(0x00000000), 0 },
            { new Timestamp(0x00000000), new Timestamp(0x00000001), -1 },
            { new Timestamp(0x00000000), new Timestamp(0x7fffffff), -1 },
            { new Timestamp(0x00000000), new Timestamp(0x80000000), null },
            { new Timestamp(0x00000000), new Timestamp(0xffffffff), 1 },

            { new Timestamp(0x00000000), new Timestamp(0x00000000), 0 },
            { new Timestamp(0x00000001), new Timestamp(0x00000000), 1 },
            { new Timestamp(0x7fffffff), new Timestamp(0x00000000), 1 },
            { new Timestamp(0x80000000), new Timestamp(0x00000000), null },
            { new Timestamp(0xffffffff), new Timestamp(0x00000000), -1 },

            { new Timestamp(0x7ffffffe), new Timestamp(0x7fffffff), -1 },
            { new Timestamp(0x7fffffff), new Timestamp(0x7ffffffe), 1 },
            { new Timestamp(0x7fffffff), new Timestamp(0x80000000), -1 },
            { new Timestamp(0x80000000), new Timestamp(0x7fffffff), 1 },

            { new Timestamp(0x39d7f21e), new Timestamp(0x39d7f21e), 0 },
            { new Timestamp(0x39d7f21e), new Timestamp(0x39d7f21e + 0x7fffffff), -1 },
            { new Timestamp(0x39d7f21e), new Timestamp(0x39d7f21e + 0x80000000), null },
            { new Timestamp(0x39d7f21e), new Timestamp(0x39d7f21e + 0x80000001), 1 },

            { new Timestamp(0xa9d7f21e), new Timestamp(0xa9d7f21e), 0 },
            { new Timestamp(0xa9d7f21e), new Timestamp(0xa9d7f21e + 0x7fffffff), -1 },
            { new Timestamp(0xa9d7f21e), new Timestamp(0xa9d7f21e + 0x80000000), null },
            { new Timestamp(0xa9d7f21e), new Timestamp(0xa9d7f21e + 0x80000001), 1 },
        };
    }
}

