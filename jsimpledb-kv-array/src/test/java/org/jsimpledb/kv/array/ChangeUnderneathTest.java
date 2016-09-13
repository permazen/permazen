
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.array;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.mvcc.Writes;
import org.jsimpledb.kv.test.KVDatabaseTest;
import org.jsimpledb.kv.test.KVTestSupport;
import org.jsimpledb.util.ByteUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class ChangeUnderneathTest extends KVTestSupport {

    private static final byte[] KEY1 = new byte[] { (byte)0x10 };
    private static final byte[] KEY2 = new byte[] { (byte)0x20 };
    private static final byte[] KEY3 = new byte[] { (byte)0x18 };

    private static final byte[] VAL1 = new byte[] { (byte)0xee };
    private static final byte[] VAL2 = new byte[] { (byte)0xff };
    private static final byte[] VAL3 = new byte[] { (byte)0xaa };

    @Test
    public void testChangeUnderneath() throws Exception {

        // Create k/v store
        final AtomicArrayKVStore kvstore = new AtomicArrayKVStore();
        final File dir = File.createTempFile(this.getClass().getSimpleName() + "-", null);
        Assert.assertTrue(dir.delete());
        Assert.assertTrue(dir.mkdirs());
        dir.deleteOnExit();
        kvstore.setDirectory(dir);
        kvstore.start();
        try {

        // Initialize kvstore with (KEY1, VAL1)

            final Writes mods = new Writes();
            mods.getPuts().put(KEY1, VAL1);
            kvstore.mutate(mods, true);
            Assert.assertEquals(stringView(this.asMap(kvstore)), buildMap(
              s(KEY1), s(VAL1)));

        // Create and verify snapshot

            final MutableView snap = new MutableView(kvstore.snapshot());
            Assert.assertEquals(stringView(this.asMap(snap)), buildMap(
              s(KEY1), s(VAL1)));

        // Add (KEY2, VAL2) to kvstore and verify

            mods.clear();
            mods.getPuts().put(KEY2, VAL2);
            kvstore.mutate(mods, true);

            Assert.assertEquals(stringView(this.asMap(kvstore)), buildMap(
              s(KEY1), s(VAL1), s(KEY2), s(VAL2)));
            Assert.assertEquals(stringView(this.asMap(snap)), buildMap(
              s(KEY1), s(VAL1)));

        // Add (KEY2, VAL3) to snapshot and verify

            snap.put(KEY2, VAL3);

            Assert.assertEquals(stringView(this.asMap(kvstore)), buildMap(
              s(KEY1), s(VAL1), s(KEY2), s(VAL2)));
            Assert.assertEquals(stringView(this.asMap(snap)), buildMap(
              s(KEY1), s(VAL1), s(KEY2), s(VAL3)));

        // Remove (KEY2, VAL2) from kvstore and verify

            mods.clear();
            mods.getRemoves().add(new KeyRange(KEY2, null));
            kvstore.mutate(mods, true);

            Assert.assertEquals(stringView(this.asMap(kvstore)), buildMap(
              s(KEY1), s(VAL1)));
            Assert.assertEquals(stringView(this.asMap(snap)), buildMap(
              s(KEY1), s(VAL1), s(KEY2), s(VAL3)));

        // Compact

            kvstore.scheduleCompaction();
            Thread.sleep(1000);

        // Verify again

            Assert.assertEquals(stringView(this.asMap(kvstore)), buildMap(
              s(KEY1), s(VAL1)));
            Assert.assertEquals(stringView(this.asMap(snap)), buildMap(
              s(KEY1), s(VAL1), s(KEY2), s(VAL3)));

            Assert.assertEquals(snap.getAtLeast(KEY3), new KVPair(KEY2, VAL3));
            Assert.assertEquals(kvstore.getAtLeast(KEY3), null);

        } finally {
            kvstore.stop();
        }
    }

    private TreeMap<byte[], byte[]> asMap(KVStore kvstore) {
        final TreeMap<byte[], byte[]> map = new TreeMap<>(ByteUtil.COMPARATOR);
        for (Iterator<KVPair> i = kvstore.getRange(null, null, false); i.hasNext(); ) {
            final KVPair pair = i.next();
            map.put(pair.getKey(), pair.getValue());
        }
        return map;
    }
}
