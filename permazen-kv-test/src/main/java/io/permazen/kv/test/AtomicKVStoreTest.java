
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.test;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.Writes;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.LongEncoder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public abstract class AtomicKVStoreTest extends KVTestSupport {

    private static final ByteData KEY1 = ByteData.of(0x10);
    private static final ByteData KEY2 = ByteData.of(0x20);
    private static final ByteData KEY3 = ByteData.of(0x18);

    private static final ByteData VAL1 = ByteData.of(0xee);
    private static final ByteData VAL2 = ByteData.of(0xff);
    private static final ByteData VAL3 = ByteData.of(0xaa);

    private File dir;

    @AfterClass
    public void cleanup() throws IOException {
        Files.walkFileTree(this.dir.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @DataProvider(name = "kvstores")
    public AtomicKVStore[][] genAtomicKVStores() throws Exception {

        // Create directory
        this.dir = File.createTempFile(this.getClass().getSimpleName(), null);
        if (!this.dir.delete() || !this.dir.mkdirs())
            throw new IOException(String.format("can't create %s", dir));
        final ArrayList<AtomicKVStore> list = new ArrayList<>();

        // Get AtomicKVStore(s)
        this.addAtomicKVStores(dir, list);

        // Build array
        final AtomicKVStore[][] array = new AtomicKVStore[list.size()][];
        for (int i = 0; i < array.length; i++)
            array[i] = new AtomicKVStore[] { list.get(i) };
        return array;
    }

    protected void addAtomicKVStores(File dir, List<AtomicKVStore> list) throws Exception {
        list.add(this.createAtomicKVStore(dir));
    }

    protected abstract AtomicKVStore createAtomicKVStore(File dir) throws Exception;

    @Test(dataProvider = "kvstores")
    public void testAtomicKVStore(AtomicKVStore kv) throws Exception {

        // Start kvstore
        kv.start();

        // Test
        final TreeMap<ByteData, ByteData> map = new TreeMap<>();
        for (int count = 0; count < 200; count++) {
            this.log.trace("[{}] next iteration", count);
            Writes writes;

            // Do puts atomically
            writes = this.getPuts(count, map);
            kv.apply(writes, true);
            this.compare(this.read(count, kv), map);
            Thread.sleep(5);

            // Do removes non-atomically
            writes = this.getRemoves(count, map);
            writes.applyTo(kv);
            this.compare(this.read(count, kv), map);
            Thread.sleep(5);

            // Do puts non-atomically
            writes = this.getPuts(count, map);
            writes.applyTo(kv);
            this.compare(this.read(count, kv), map);
            Thread.sleep(5);

            // Do removes atomically
            writes = this.getRemoves(count, map);
            kv.apply(writes, true);
            this.compare(this.read(count, kv), map);
            Thread.sleep(5);
        }

        // Stop kvstore
        kv.stop();
    }

    @Test(dataProvider = "kvstores")
    public void testChangeUnderneath(AtomicKVStore kvstore) throws Exception {

        // Start kvstore

        kvstore.start();

        // Initialize kvstore with (KEY1, VAL1)

        final Writes mods = new Writes();
        mods.getPuts().put(KEY1, VAL1);
        kvstore.apply(mods, true);
        this.log.debug("step1: kvstore={}", stringView(this.asMap(kvstore)));
        Assert.assertEquals(stringView(this.asMap(kvstore)), buildMap(
          s(KEY1), s(VAL1)));

        // Create and verify snapshot

        final CloseableKVStore snapshot = kvstore.readOnlySnapshot();
        final MutableView view = new MutableView(snapshot);
        this.log.debug("step2: kvstore={} view={}", stringView(this.asMap(kvstore)), stringView(this.asMap(view)));
        Assert.assertEquals(stringView(this.asMap(view)), buildMap(
          s(KEY1), s(VAL1)));

        // Add (KEY2, VAL2) to kvstore and verify

        mods.clear();
        mods.getPuts().put(KEY2, VAL2);
        kvstore.apply(mods, true);

        this.log.debug("step3: kvstore={} view={}", stringView(this.asMap(kvstore)), stringView(this.asMap(view)));
        Assert.assertEquals(stringView(this.asMap(kvstore)), buildMap(
          s(KEY1), s(VAL1), s(KEY2), s(VAL2)));
        Assert.assertEquals(stringView(this.asMap(view)), buildMap(
          s(KEY1), s(VAL1)));

        // Add (KEY2, VAL3) to view and verify

        view.put(KEY2, VAL3);

        this.log.debug("step4: kvstore={} view={}", stringView(this.asMap(kvstore)), stringView(this.asMap(view)));
        Assert.assertEquals(stringView(this.asMap(kvstore)), buildMap(
          s(KEY1), s(VAL1), s(KEY2), s(VAL2)));
        Assert.assertEquals(stringView(this.asMap(view)), buildMap(
          s(KEY1), s(VAL1), s(KEY2), s(VAL3)));

        // Remove (KEY2, VAL2) from kvstore and verify

        mods.clear();
        mods.getRemoves().add(new KeyRange(KEY2, null));
        kvstore.apply(mods, true);

        this.log.debug("step5: kvstore={} view={}", stringView(this.asMap(kvstore)), stringView(this.asMap(view)));
        Assert.assertEquals(stringView(this.asMap(kvstore)), buildMap(
          s(KEY1), s(VAL1)));
        Assert.assertEquals(stringView(this.asMap(view)), buildMap(
          s(KEY1), s(VAL1), s(KEY2), s(VAL3)));

        // Compact

        this.compact(kvstore);

        // Verify again

        this.log.debug("step6: kvstore={} view={}", stringView(this.asMap(kvstore)), stringView(this.asMap(view)));
        Assert.assertEquals(stringView(this.asMap(kvstore)), buildMap(
          s(KEY1), s(VAL1)));
        Assert.assertEquals(stringView(this.asMap(view)), buildMap(
          s(KEY1), s(VAL1), s(KEY2), s(VAL3)));

        Assert.assertEquals(view.getAtLeast(KEY3, null), new KVPair(KEY2, VAL3));
        Assert.assertEquals(kvstore.getAtLeast(KEY3, null), null);

        // Done

        snapshot.close();
        kvstore.stop();
    }

    protected void compact(AtomicKVStore kvstore) throws Exception {
        // Subclass can do something here
    }

    private Writes getPuts(int count, TreeMap<ByteData, ByteData> map) {
        final Writes writes = new Writes();
        for (int i = 0; i < 16; i++) {
            final ByteData key = ByteData.of((byte)this.random.nextInt(0xff));
            final ByteData key2 = ByteData.of((byte)this.random.nextInt(0xff), (byte)this.random.nextInt(0xff));
            final ByteData value = LongEncoder.encode((1 << i) + i);
            this.log.trace("[{}]: PUT: {} -> {}", count, ByteUtil.toString(key), ByteUtil.toString(value));
            writes.getPuts().put(key, value);
            this.log.trace("[{}]: PUT: {} -> {}", count, ByteUtil.toString(key2), ByteUtil.toString(value));
            writes.getPuts().put(key2, value);
            map.put(key, value);
            map.put(key2, value);
        }
        return writes;
    }

    private Writes getRemoves(int count, TreeMap<ByteData, ByteData> map) {
        final Writes writes = new Writes();
        for (int i = 0; i < 9; i++) {
            if (this.random.nextInt(5) > 0) {
                final ByteData key = ByteData.of((byte)this.random.nextInt(0xff));
                this.log.trace("[{}]: REMOVE: {}", count, ByteUtil.toString(key));
                writes.getRemoves().add(new KeyRange(key));
                map.remove(key);
            } else {
                final ByteData x = this.random.nextInt(10) == 0 ? ByteData.empty() : ByteData.of((byte)this.random.nextInt(0xff));
                final ByteData y = this.random.nextInt(10) == 0 ? null : ByteData.of((byte)this.random.nextInt(0xff));
                final ByteData minKey = y == null || x.compareTo(y) < 0 ? x : y;
                final ByteData maxKey = y == null || x.compareTo(y) < 0 ? y : x;
                this.log.trace("[{}]: REMOVE: [{}, {})", count, ByteUtil.toString(minKey), ByteUtil.toString(maxKey));
                writes.getRemoves().add(new KeyRange(minKey, maxKey));
                if (maxKey == null)
                    map.tailMap(minKey, true).clear();
                else
                    map.subMap(minKey, true, maxKey, false).clear();
            }
        }
        return writes;
    }

    private TreeMap<ByteData, ByteData> read(int count, AtomicKVStore kv) {
        return this.read(count, kv, ByteData.empty(), null);
    }

    private TreeMap<ByteData, ByteData> read(int count, AtomicKVStore lkv, ByteData minKey, ByteData maxKey) {
        final TreeMap<ByteData, ByteData> map = new TreeMap<>();
        this.log.trace("[{}]: reading kv store", count);
        final KVStore kv;
        final CloseableKVStore snapshot;
        if (this.random.nextBoolean()) {
            snapshot = lkv.readOnlySnapshot();
            kv = snapshot;
        } else {
            snapshot = null;
            kv = lkv;
        }
        try {
            try (CloseableIterator<KVPair> i = kv.getRange(minKey, maxKey)) {
                while (i.hasNext()) {
                    final KVPair pair = i.next();
                    map.put(pair.getKey(), pair.getValue());
                }
            }
        } finally {
            if (snapshot != null)
                snapshot.close();
        }
        return map;
    }

    private void compare(TreeMap<ByteData, ByteData> map1, TreeMap<ByteData, ByteData> map2) {
        final NavigableMap<String, String> smap1 = stringView(map1);
        final NavigableMap<String, String> smap2 = stringView(map2);
        Assert.assertEquals(smap1, smap2, "\n*** ACTUAL:\n" + smap1 + "\n*** EXPECTED:\n" + smap2 + "\n");
    }

    private TreeMap<ByteData, ByteData> asMap(KVStore kvstore) {
        final TreeMap<ByteData, ByteData> map = new TreeMap<>();
        try (CloseableIterator<KVPair> i = kvstore.getRange(null, null)) {
            while (i.hasNext()) {
                final KVPair pair = i.next();
                map.put(pair.getKey(), pair.getValue());
            }
        }
        return map;
    }
}
