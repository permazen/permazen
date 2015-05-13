
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.mvcc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.jsimpledb.TestSupport;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
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
        final Reads reads2 = Reads.deserialize(input);
        Assert.assertEquals(reads2.getReads(), reads.getReads());
        final ByteArrayOutputStream output2 = new ByteArrayOutputStream();
        reads2.serialize(output2);
        Assert.assertEquals(output2.toByteArray(), output.toByteArray());
    }

    @DataProvider(name = "ranges")
    private KeyRanges[][] genReads() throws Exception {
        return new KeyRanges[][] {
            { KeyRanges.EMPTY },
            { KeyRanges.FULL },
            { new KeyRanges(KeyRange.forPrefix(b("0123")), KeyRange.forPrefix(b("33"))) },
            { new KeyRanges(
              new KeyRange(b(""), b("ffff")),
              new KeyRange(b("ffffff"), null))
            },
            { new KeyRanges(
              new KeyRange(b("01"), b("0134")),
              new KeyRange(b("022222"), b("0223")),
              new KeyRange(b("30"), b("300000")),
              new KeyRange(b("3333333333"), b("33333333333333")),
              new KeyRange(b("4433333333"), b("44333344333333")),
              new KeyRange(b("33433333"), b("434343")),
              new KeyRange(b("99999999"), b("c0")))
            },
            { new KeyRanges(b("01234567890a"), b("0123456789ff")) },
            { new KeyRanges(b("01234567890a"), b("01234567ffff")) },
            { new KeyRanges(b("01234567890a"), b("012345ffffff")) },
            { new KeyRanges(b("01234567890a"), b("0123ffffffff")) },
            { new KeyRanges(b("01234567890a"), b("01ffffffffff")) },
            { new KeyRanges(b("01234567890a"), b("ffffffffffff")) },
        };
    };
}

