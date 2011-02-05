
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.string;

import org.dellroad.stuff.TestSupport;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ByteArrayEncoderTest extends TestSupport {

    @Test(dataProvider = "data")
    public void testByteArrayEncoding(String input, byte[] data, String output) {
        if (data == null) {
            try {
                ByteArrayEncoder.decode(input);
                assert false;
            } catch (IllegalArgumentException e) {
                return;
            }
        }
        byte[] data2 = ByteArrayEncoder.decode(input);
        assertEquals(data2, data);
        assertEquals(ByteArrayEncoder.encode(data2), output);
    }

    @DataProvider(name = "data")
    public Object[][] genData1() {
        return new Object[][] {

            // valid
            { "",                           new byte[0],                                ""              },
            { "  ",                         new byte[0],                                ""              },
            { " \t\f\r\n",                  new byte[0],                                ""              },
            { "12",                         new byte[] { (byte)0x12 },                  "12"            },
            { "AB",                         new byte[] { (byte)0xab },                  "ab"            },
            { "AB",                         new byte[] { (byte)0xab },                  "ab"            },
            { " 1 2 3 4 ",                  new byte[] { (byte)0x12, (byte)0x34 },      "1234"          },
            { "ac\ndc\n\n",                 new byte[] { (byte)0xac, (byte)0xdc },      "acdc"          },

            // invalid
            { "0",                          null,   null    },
            { "1g",                         null,   null    },
            { "3+",                         null,   null    },
            { "987654321",                  null,   null    },
            { "+00",                        null,   null    },
            { "-00",                        null,   null    },
        };
    }
}

