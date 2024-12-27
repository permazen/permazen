
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.test.TestSupport;
import io.permazen.util.ByteData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReadsTest extends TestSupport {

    @Test(dataProvider = "ranges")
    public void testSerializeReads(KeyRanges ranges) throws Exception {
        final Reads reads = new Reads(ranges);
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        reads.serialize(output);
        final ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
        final Reads reads2 = new Reads(input);
        Assert.assertEquals(reads2, reads);
        final ByteArrayOutputStream output2 = new ByteArrayOutputStream();
        reads2.serialize(output2);
        Assert.assertEquals(output2.toByteArray(), output.toByteArray());
    }

    @DataProvider(name = "ranges")
    private KeyRanges[][] genReads() throws Exception {
        return new KeyRanges[][] {
            { KeyRanges.empty() },
            { KeyRanges.full() },
            { new KeyRanges(KeyRange.forPrefix(ByteData.fromHex("0123")), KeyRange.forPrefix(ByteData.fromHex("33"))) },
            { new KeyRanges(
              new KeyRange(ByteData.fromHex(""), ByteData.fromHex("ffff")),
              new KeyRange(ByteData.fromHex("ffffff"), null))
            },
            { new KeyRanges(
              new KeyRange(ByteData.fromHex("01"), ByteData.fromHex("0134")),
              new KeyRange(ByteData.fromHex("022222"), ByteData.fromHex("0223")),
              new KeyRange(ByteData.fromHex("30"), ByteData.fromHex("300000")),
              new KeyRange(ByteData.fromHex("3333333333"), ByteData.fromHex("33333333333333")),
              new KeyRange(ByteData.fromHex("4433333333"), ByteData.fromHex("44333344333333")),
              new KeyRange(ByteData.fromHex("33433333"), ByteData.fromHex("434343")),
              new KeyRange(ByteData.fromHex("99999999"), ByteData.fromHex("c0")))
            },
            { new KeyRanges(ByteData.fromHex("01234567890a"), ByteData.fromHex("0123456789ff")) },
            { new KeyRanges(ByteData.fromHex("01234567890a"), ByteData.fromHex("01234567ffff")) },
            { new KeyRanges(ByteData.fromHex("01234567890a"), ByteData.fromHex("012345ffffff")) },
            { new KeyRanges(ByteData.fromHex("01234567890a"), ByteData.fromHex("0123ffffffff")) },
            { new KeyRanges(ByteData.fromHex("01234567890a"), ByteData.fromHex("01ffffffffff")) },
            { new KeyRanges(ByteData.fromHex("01234567890a"), ByteData.fromHex("ffffffffffff")) },
        };
    };
}
