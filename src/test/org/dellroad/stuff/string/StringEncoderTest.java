
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
import static org.testng.Assert.assertNull;

public class StringEncoderTest extends TestSupport {

    @Test(dataProvider = "encode")
    public void testStringEncode(boolean tabcrlf, String original, String expectedEncoding) {

        // Sanity check our input
        assert original != null || expectedEncoding != null;

        // Shorthand for identity encodings
        if (expectedEncoding == null)
            expectedEncoding = original;

        // Encode original and compare with expected
        if (original != null) {
            String encoding = StringEncoder.encode(original, tabcrlf);
            assertEquals(encoding, expectedEncoding);
        }

        // Decode back and compare with original
        String decoding;
        try {
            decoding = StringEncoder.decode(expectedEncoding);
            assertEquals(decoding, original);
        } catch (IllegalArgumentException e) {
            if (original != null)
                throw e;
        }
    }

    @DataProvider(name = "encode")
    public Object[][] genEncodeCases() {
        return new Object[][] {

            // Identity encodings
            new Object[] { false, "", null },
            new Object[] { false, " !@#$%^&*()_+=asdfajsADSFASDF298734?></.,~`][}|}", null },
            new Object[] { false, " \t\r\n ", null },

            // Other valid encodings
            new Object[] { true,  " \t\r\n ", " \\t\\r\\n " },
            new Object[] { false, " \b\f\\ ", " \\b\\f\\\\ " },
            new Object[] { true,  " \b\t\\ ", " \\b\\t\\\\ " },
            new Object[] { false, "\\", "\\\\" },
            new Object[] { false, "\n", "\n" },
            new Object[] { true,  "\n", "\\n" },
            new Object[] { false, "\\foo\\", "\\\\foo\\\\" },
            new Object[] { false, "foo\\bar", "foo\\\\bar" },
            new Object[] { false,
              new String(new char[] {
               (char)0x0008, (char)0x0009, (char)0x000a, (char)0x000b, (char)0x000c, (char)0x000d, (char)0x000e,
               (char)0x001f, (char)0x0020, (char)0x0021, (char)0x1234, (char)0xd7ff, (char)0xd800, (char)0xdabc,
               (char)0xdfff, (char)0xe000, (char)0xf123, (char)0xfffd, (char)0xfffe
              }),
              "\\b" + "\t\n" + esc(0x000b) + "\\f" + "\r" + esc(0x000e)
               + esc(0x001f) + " !" + "\u1234\ud7ff" + esc(0xd800) + esc(0xdabc)
               + esc(0xdfff) + "\ue000\uf123\ufffd" + esc(0xfffe)
            },

            // Invalid encodings
            new Object[] { false, null, "foobar \\" },
            new Object[] { false, null, "foobar \\\\\\" },
            new Object[] { false, null, "foobar \\ " },
            new Object[] { false, null, "foobar \\1234" },
            new Object[] { false, null, "foobar \u005c\u005cu" },
            new Object[] { false, null, "foobar \u005c\u005cu1" },
            new Object[] { false, null, "foobar \u005c\u005cu12" },
            new Object[] { false, null, "foobar \u005c\u005cu123" },
            new Object[] { false, null, "foobar \u005c\u005cu123g" },
            new Object[] { false, null, "foobar \u005c\u005cuz000" },
        };
    }

    @Test(dataProvider = "quote")
    public void testStringEnquote(String original, String expectedEnquoting) {

        // Sanity check our input
        assert original != null || expectedEnquoting != null;

        // Encode original and compare with expected
        if (original != null) {
            String enquoted = StringEncoder.enquote(original);
            assertEquals(enquoted, expectedEnquoting);
        }

        // Decode back and compare with original
        String dequoted;
        try {
            dequoted = StringEncoder.dequote(expectedEnquoting);
            assertEquals(dequoted, original);
        } catch (IllegalArgumentException e) {
            if (original != null)
                throw e;
        }
    }

    @DataProvider(name = "quote")
    public Object[][] genQuoteCases() {
        return new Object[][] {

            // Valid enquotings
            { "fred", "\"fred\"" },
            { "field1\tfield2", "\"field1\\tfield2\"" },
            { "a \"quote\"", "\"a \\\"quote\\\"\"" },
            { "\"No\" Lard", "\"\\\"No\\\" Lard\"" },

            // Invalid enquotings
            { null, "fred" },
            { null, "\\\"missing quote" },
            { null, "missing quote\\\"" },
            { null, "\\\"impossible \"embeded but not escaped\" quotes\\\"" },
        };
    }

    private String esc(int code) {
        assert code >= 0 && code <= 0xffff;
        return "\\" + "u" + String.format("%04x", code);
    }
}

