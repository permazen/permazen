
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import com.google.common.collect.Lists;

import io.permazen.kv.KVPair;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.test.AtomicKVStoreTest;
import io.permazen.kv.util.NavigableMapKVStore;
import io.permazen.util.ByteUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ArrayKVStoreTest extends AtomicKVStoreTest {

    @Test
    private void testArrayKVStore() throws Exception {
        for (int i = 0; i < 1000; i++) {

            // Build a KVStore with random data in it
            final int maxKeyLen = 5;
            final ByteArrayOutputStream indxOutput = new ByteArrayOutputStream();
            final ByteArrayOutputStream keysOutput = new ByteArrayOutputStream();
            final ByteArrayOutputStream valsOutput = new ByteArrayOutputStream();
            final byte[] keybuf = new byte[maxKeyLen];
            final byte[] maxkey = new byte[maxKeyLen];
            Arrays.fill(maxkey, (byte)0xff);
            int keylen = 0;
            byte[] key;
            final NavigableMapKVStore reference = new NavigableMapKVStore();
            final ArrayKVWriter writer = new ArrayKVWriter(indxOutput, keysOutput, valsOutput);
            while (true) {

                // Create key
                key = new byte[keylen];
                System.arraycopy(keybuf, 0, key, 0, keylen);

                // Add key/value pair (maybe)
                if (this.random.nextInt(5) != 3) {
                    byte[] val = new byte[this.random.nextInt(32)];
                    if (val.length > 0)
                        this.random.nextBytes(val);
                    //this.log.info("NEXT KV: {} VALUE {}", ByteUtil.toString(key), ByteUtil.toString(val));
                    writer.writeKV(key, val);
                    reference.put(key, val);
                }

                // Advance key somewhat randomly
                final int newlen = this.random.nextInt(keybuf.length - 1) + 1;
                if (newlen > keylen) {
                    while (keylen < newlen)
                        keybuf[keylen++] = (byte)(this.random.nextInt() & this.random.nextInt());
                } else {
                    keylen = newlen;
                    while (keylen > 0 && keybuf[keylen - 1] == (byte)0xff)
                        keylen--;
                    if (keylen == 0)
                        break;
                    final byte orig = keybuf[keylen - 1];
                    assert orig != (byte)0xff;
                    while (keybuf[keylen - 1] == orig)
                        keybuf[keylen - 1] |= (byte)this.random.nextInt();
                }
            }
            writer.close();
            final ArrayKVStore kvstore = new ArrayKVStore(
              ByteBuffer.wrap(indxOutput.toByteArray()),
              ByteBuffer.wrap(keysOutput.toByteArray()),
              ByteBuffer.wrap(valsOutput.toByteArray()));

            // Debug
            //this.log.info("INDX:{}", this.format(indxOutput.toByteArray()));
            //this.log.info("KEYS:{}", this.format(keysOutput.toByteArray()));
            //this.log.info("VALS:{}", this.format(valsOutput.toByteArray()));

            // Perform a bunch of queries and verify we get the same thing from both
            for (int j = 0; j < 100; j++) {
                key = this.randomKey(maxKeyLen);
                final byte[] key2 = this.random.nextInt(100) < 75 ? null : this.randomKey(maxKeyLen);
                int option = this.random.nextInt(40);
                if (option < 10)
                    this.verify(kvstore.get(key), reference.get(key));
                else if (option < 20)
                    this.verify(kvstore.getAtLeast(key, key2), reference.getAtLeast(key, key2));
                else if (option < 30)
                    this.verify(kvstore.getAtMost(key, key2), reference.getAtMost(key, key2));
                else {
                    byte[] minKey = this.random.nextInt(5) == 3 ? null : key;
                    byte[] maxKey = this.random.nextInt(5) == 3 ? null : this.randomKey(maxKeyLen * 2);
                    if (minKey != null && maxKey != null && ByteUtil.compare(minKey, maxKey) > 0) {
                        byte[] temp = minKey;
                        minKey = maxKey;
                        maxKey = temp;
                    }
                    final boolean reverse = this.random.nextBoolean();
                    this.verify(kvstore.getRange(minKey, maxKey, reverse),
                      reference.getRange(minKey, maxKey, reverse));
                }
            }
        }
    }

    @Override
    protected void compact(AtomicKVStore kvstore) throws Exception {
        ((AtomicArrayKVStore)kvstore).scheduleCompaction();
        Thread.sleep(1000);
    }

    @Override
    protected AtomicArrayKVStore createAtomicKVStore(File dir) throws Exception {
        final AtomicArrayKVStore kv = new AtomicArrayKVStore();
        kv.setDirectory(dir);
        return kv;
    }

    private void verify(byte[] actual, byte[] expected) {
        Assert.assertEquals(actual, expected);
    }

    private void verify(KVPair actual, KVPair expected) {
        Assert.assertEquals(actual, expected);
    }

    private void verify(Iterator<KVPair> actual, Iterator<KVPair> expected) {
        Assert.assertEquals(Lists.newArrayList(actual), Lists.newArrayList(expected));
    }

    private byte[] randomKey(int maxKeyLen) {
        final byte[] key = new byte[this.random.nextInt(maxKeyLen + 1)];
        this.random.nextBytes(key);
        return key;
    }

    private String format(byte[] data) {
        final StringBuilder buf = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            switch (i % 8) {
            case 0:
                buf.append(String.format("\n%04x:  ", i));
                break;
            case 4:
                buf.append("  ");
                break;
            default:
                break;
            }
            buf.append(String.format("%02x", data[i] & 0xff));
        }
        return buf.toString();
    }
}
