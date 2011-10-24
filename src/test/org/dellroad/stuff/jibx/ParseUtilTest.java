
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;

import org.dellroad.stuff.TestSupport;

import org.jibx.runtime.JiBXException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ParseUtilTest extends TestSupport {

    @Test(dataProvider = "timeIntervals")
    public void testParse(String input, long output) throws Exception {
        long value;
        try {
            value = ParseUtil.deserializeTimeInterval(input);
        } catch (JiBXException e) {
            value = -1;
        }
        assertEquals(value, output);
        if (value == -1)
            return;
        long value2 = ParseUtil.deserializeTimeInterval(ParseUtil.serializeTimeInterval(value));
        assertEquals(value2, value);
    }

    @DataProvider(name = "timeIntervals")
    public Object[][] getTimeIntervals() {
        return new Object[][] {

            // Valid examples
            { "2h", 2 * 60 * 60 * 1000 },
            { "7.254s", 7254 },
            { "3d", 3 * 24 * 60 * 60 * 1000 },
            { "3d17h4m3.01s", 3 * 24 * 60 * 60 * 1000
              + 17 * 60 * 60 * 1000 + 4 * 60 * 1000 + 3010 },
            { "1.001s", 1001 },
            { "1.03s", 1030 },
            { "1.3s", 1300 },
            { "9.300s", 9300 },
            { "6s", 6000 },
            { ".503s", 503 },
            { ".999s", 999 },
            { "1.000s", 1000 },
            { "1s", 1000 },
            { "1.001s", 1001 },
            { "180d", 15552000000L },
            { "106751991167d7h12m55.807s", Long.MAX_VALUE },
            { "106751991167d7h12m55.808s", -1 },

            // Invalid examples
            { "123", -1 },
            { "7d3m15h", -1 },
            { "123.456", -1 },
            { "3m2h", -1 },
            { "15 s", -1 },
            { "0.5h", -1 },
            { "asdf", -1 },
            { "", -1 },
            { "s", -1 },
            { "35ms", -1 }
        };
    }
}

