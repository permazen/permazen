
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Converter;

import io.permazen.kv.KVPair;
import io.permazen.test.TestSupport;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.ConvertedNavigableMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.NavigableMap;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyListEncoderTest extends TestSupport {

    @Test
    private void testEncodeDecode() throws Exception {
        for (int count = 0; count < 1000; count++) {

            // Populate k/v store with random data
            final MemoryKVStore original = new MemoryKVStore();
            for (int j = 0; j < this.random.nextInt(100); j++) {
                final ByteData key = this.rb(1 << this.random.nextInt(6));
                final ByteData val = this.rb(1 << this.random.nextInt(12));
                original.put(key, val);
            }

            // Encode
            final ByteArrayOutputStream buf = new ByteArrayOutputStream();
            final long length = KeyListEncoder.writePairsLength(original.getRange(null, null));
            KeyListEncoder.writePairs(original.getRange(null, null), buf);

            // Check length
            Assert.assertEquals(buf.size(), length);

            // Decode
            final MemoryKVStore copy = new MemoryKVStore();
            for (Iterator<KVPair> i = KeyListEncoder.readPairs(new ByteArrayInputStream(buf.toByteArray())); i.hasNext(); ) {
                final KVPair kv = i.next();
                copy.put(kv.getKey(), kv.getValue());
            }

            // Check same result
            Assert.assertEquals(this.stringView(copy.getNavigableMap()), this.stringView(original.getNavigableMap()));
        }
    }

    private ByteData rb(int len) {
        final byte[] b = new byte[this.random.nextInt(len) + 1];
        this.random.nextBytes(b);
        return ByteData.of(b);
    }

    private NavigableMap<String, String> stringView(NavigableMap<ByteData, ByteData> byteMap) {
        if (byteMap == null)
            return null;
        final Converter<String, ByteData> converter = ByteUtil.STRING_CONVERTER.reverse();
        return new ConvertedNavigableMap<>(byteMap, converter, converter);
    }
}
