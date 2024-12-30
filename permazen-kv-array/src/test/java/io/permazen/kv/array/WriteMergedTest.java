
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import com.google.common.collect.Lists;

import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.util.MemoryKVStore;
import io.permazen.test.TestSupport;
import io.permazen.util.ByteData;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.testng.Assert;
import org.testng.annotations.Test;

public class WriteMergedTest extends TestSupport {

    @Test
    private void testWriteMerged() throws Exception {

        // Setup k/v store
        final MemoryKVStore kvstore = new MemoryKVStore();
        kvstore.put(ByteData.fromHex("0001"), ByteData.fromHex("aaaa"));
        kvstore.put(ByteData.fromHex("000101"), ByteData.fromHex("bbbb"));
        kvstore.put(ByteData.fromHex("000102"), ByteData.fromHex("cccc"));
        kvstore.put(ByteData.fromHex("0002"), ByteData.fromHex("dddd"));

        // Setup mutations
        final MutableView view = new MutableView(kvstore);
        view.put(ByteData.fromHex("000101"), ByteData.fromHex("eeee"));
        view.put(ByteData.fromHex("000102"), ByteData.fromHex("ffff"));
        view.removeRange(ByteData.fromHex("0001"), ByteData.fromHex("0002"));

        // Merge
        final ByteArrayOutputStream indxBuf = new ByteArrayOutputStream();
        final ByteArrayOutputStream keysBuf = new ByteArrayOutputStream();
        final ByteArrayOutputStream valsBuf = new ByteArrayOutputStream();
        final ArrayKVWriter writer = new ArrayKVWriter(indxBuf, keysBuf, valsBuf);
        writer.writeMerged(kvstore, kvstore.getRange(null, null, false), view.getWrites());
        writer.close();
        final ArrayKVStore actual = new ArrayKVStore(
          ByteBuffer.wrap(indxBuf.toByteArray()),
          ByteBuffer.wrap(keysBuf.toByteArray()),
          ByteBuffer.wrap(valsBuf.toByteArray()));

        // Verify result
        Assert.assertEquals(
          Lists.newArrayList(actual.getRange(null, null, false)).toString(),
          Lists.newArrayList(view.getRange(null, null, false)).toString());
        view.getWrites().applyTo(kvstore);
        Assert.assertEquals(
          Lists.newArrayList(actual.getRange(null, null, false)).toString(),
          Lists.newArrayList(kvstore.getRange(null, null, false)).toString());
    }
}
