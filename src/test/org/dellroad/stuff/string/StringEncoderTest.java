
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.string;

import org.dellroad.stuff.TestSupport;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
            Assert.assertTrue(StringEncoder.ENCODE_PATTERN.matcher(encoding).matches());
            Assert.assertEquals(encoding, expectedEncoding);
        }

        // Decode back and compare with original
        String decoding;
        try {
            decoding = StringEncoder.decode(expectedEncoding);
            Assert.assertEquals(decoding, original);
            Assert.assertTrue(StringEncoder.ENCODE_PATTERN.matcher(expectedEncoding).matches());
        } catch (IllegalArgumentException e) {
            Assert.assertFalse(StringEncoder.ENCODE_PATTERN.matcher(expectedEncoding).matches());
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
            Assert.assertTrue(StringEncoder.ENQUOTE_PATTERN.matcher(enquoted).matches());
            Assert.assertEquals(enquoted, expectedEnquoting);
        }

        // Decode back and compare with original
        String dequoted;
        try {
            dequoted = StringEncoder.dequote(expectedEnquoting);
            Assert.assertTrue(StringEncoder.ENQUOTE_PATTERN.matcher(expectedEnquoting).matches());
            Assert.assertEquals(dequoted, original);
        } catch (IllegalArgumentException e) {
            Assert.assertFalse(StringEncoder.ENQUOTE_PATTERN.matcher(expectedEnquoting).matches());
            if (original != null)
                throw e;
        }
    }

    @Test
    public void testRandom() {
        for (int i = 0; i < 10000; i++) {
            final int len = this.random.nextInt(50) + 1;
            final char[] array = new char[len];
            for (int j = 0; j < len; j++)
                array[j] = (char)((this.random.nextInt(0x100) << 16) | this.random.nextInt(0x100));
            final String s1 = new String(array);

            // Check encoding
            final String encoded = StringEncoder.encode(s1, this.random.nextBoolean());
            Assert.assertTrue(StringEncoder.ENCODE_PATTERN.matcher(encoded).matches(),
              "encoding doesn't match pattern: " + encoded);
            final String s2 = StringEncoder.decode(encoded);
            Assert.assertEquals(s2, s1, "failed with encoding " + encoded);
            final int modpos1 = this.random.nextInt(encoded.length());
            final String encoded2 = encoded.substring(0, modpos1)
              + String.valueOf((char)this.random.nextInt(0x80)) + encoded.substring(modpos1 + 1);
            try {
                StringEncoder.decode(encoded2);
                Assert.assertTrue(StringEncoder.ENCODE_PATTERN.matcher(encoded2).matches(),
                  "encoding doesn't match pattern: " + encoded2);
            } catch (IllegalArgumentException e) {
                Assert.assertFalse(StringEncoder.ENCODE_PATTERN.matcher(encoded2).matches(),
                  "encoding matches pattern: " + encoded2);
            }

            // Check enquoting
            final String enquoted = StringEncoder.enquote(s1);
            Assert.assertTrue(StringEncoder.ENQUOTE_PATTERN.matcher(enquoted).matches(),
              "enquoting doesn't match pattern: " + enquoted);
            final String s3 = StringEncoder.dequote(enquoted);
            Assert.assertEquals(s3, s1, "failed with enquoting " + enquoted);
            final int modpos2 = this.random.nextInt(enquoted.length());
            final String enquoted2 = enquoted.substring(0, modpos2)
              + String.valueOf((char)this.random.nextInt(0x80)) + enquoted.substring(modpos2 + 1);
            try {
                StringEncoder.dequote(enquoted2);
                Assert.assertTrue(StringEncoder.ENQUOTE_PATTERN.matcher(enquoted2).matches(),
                  "enquoting doesn't match pattern: " + enquoted2);
            } catch (IllegalArgumentException e) {
                Assert.assertFalse(StringEncoder.ENQUOTE_PATTERN.matcher(enquoted2).matches(),
                  "enquoting matches pattern: " + enquoted2);
            }
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
            Assert.assertEquals(enquotedLength, expectedLength);
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

