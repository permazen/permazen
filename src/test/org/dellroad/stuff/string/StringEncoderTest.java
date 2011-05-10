
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
            { false, "", null },
            { false, " !@#$%^&*()_+=asdfajsADSFASDF298734?></.,~`][}|}", null },
            { false, " \t\r\n ", null },

            // Other valid encodings
            { true,  " \t\r\n ", " \\t\\r\\n " },
            { false, " \b\f\\ ", " \\b\\f\\\\ " },
            { true,  " \b\t\\ ", " \\b\\t\\\\ " },
            { false, "\\", "\\\\" },
            { false, "\n", "\n" },
            { true,  "\n", "\\n" },
            { false, "\\foo\\", "\\\\foo\\\\" },
            { false, "foo\\bar", "foo\\\\bar" },
            { false,
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
            { false, null, "foobar \\" },
            { false, null, "foobar \\\\\\" },
            { false, null, "foobar \\ " },
            { false, null, "foobar \\1234" },
            { false, null, "foobar \u005c\u005cu" },
            { false, null, "foobar \u005c\u005cu1" },
            { false, null, "foobar \u005c\u005cu12" },
            { false, null, "foobar \u005c\u005cu123" },
            { false, null, "foobar \u005c\u005cu123g" },
            { false, null, "foobar \u005c\u005cuz000" },
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

    @Test(dataProvider = "enquotedLength")
    public void testEnquotedLength(String quoted, int expectedLength) {

        // Get length
        try {
            int enquotedLength = StringEncoder.enquotedLength(quoted);
            assertEquals(enquotedLength, expectedLength);
        } catch (IllegalArgumentException e) {
            if (expectedLength != -1)
                throw e;
        }
    }

    @DataProvider(name = "enquotedLength")
    public Object[][] genQuotedLengthCases() {
        return new Object[][] {

            // Valid cases
            { "\"quote\" extra", 7 },
            { "\"a \\\"quote\\\"\"", 13 },
            { "\"a \\\"quote\\\"\" then \"junk\" and \\\"more junk\\\" then", 13 },
            { "\"quote with \\\"invalid\\XXXXescape\\\"\"", 35 },

            // Inalid cases
            { "not a quote\"", -1 },
            { "not a quote\\\" either", -1 },
            { "\"not a quote", -1 },
            { "not a quote\"", -1 },
        };
    }

    private String esc(int code) {
        assert code >= 0 && code <= 0xffff;
        return "\\" + "u" + String.format("%04x", code);
    }
}

