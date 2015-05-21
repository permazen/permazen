
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.leveldb;

import com.google.common.base.Converter;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.jsimpledb.TestSupport;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.mvcc.Writes;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ConvertedNavigableMap;
import org.jsimpledb.util.LongEncoder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

public class LevelDBKVStoreTest extends TestSupport {

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

    @Test
    public void testLevelDBKVStore() throws Exception {

        // Initialize
        this.dir = File.createTempFile(this.getClass().getSimpleName(), null);
        if (!this.dir.delete() || !this.dir.mkdirs())
            throw new IOException("can't create " + dir);
        final LevelDBKVStore kv = new LevelDBKVStore(new Iq80DBFactory().open(this.dir, new Options().createIfMissing(true)));

        // Test
        final TreeMap<byte[], byte[]> map = new TreeMap<byte[], byte[]>(ByteUtil.COMPARATOR);
        for (int count = 0; count < 100; count++) {
            this.log.trace("[" + count + "] next iteration");
            Writes writes;

            // Do puts atomically
            writes = this.getPuts(count, map);
            kv.mutate(writes, true);
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
            kv.mutate(writes, true);
            this.compare(this.read(count, kv), map);
            Thread.sleep(5);
        }
    }

    private Writes getPuts(int count, TreeMap<byte[], byte[]> map) {
        final Writes writes = new Writes();
        for (int i = 0; i < 16; i++) {
            final byte[] key = new byte[] { (byte)this.random.nextInt(0x100) };
            final byte[] key2 = new byte[] { (byte)this.random.nextInt(0x100), (byte)this.random.nextInt(0x100) };
            final byte[] value = LongEncoder.encode((1 << i) + i);
            this.log.trace("[" + count + "]: PUT: " + ByteUtil.toString(key) + " -> " + ByteUtil.toString(value));
            writes.getPuts().put(key, value);
            this.log.trace("[" + count + "]: PUT: " + ByteUtil.toString(key2) + " -> " + ByteUtil.toString(value));
            writes.getPuts().put(key2, value);
            map.put(key, value);
            map.put(key2, value);
        }
        return writes;
    }

    private Writes getRemoves(int count, TreeMap<byte[], byte[]> map) {
        final Writes writes = new Writes();
        for (int i = 0; i < 9; i++) {
            if (this.random.nextInt(5) > 0) {
                final byte[] key = new byte[] { (byte)this.random.nextInt(0x100) };
                this.log.trace("[" + count + "]: REMOVE: " + ByteUtil.toString(key));
                writes.setRemoves(writes.getRemoves().add(new KeyRange(key)));
                map.remove(key);
            } else {
                final byte[] x = this.random.nextInt(10) == 0 ? new byte[0] : new byte[] { (byte)this.random.nextInt(0x100) };
                final byte[] y = this.random.nextInt(10) == 0 ? null : new byte[] { (byte)this.random.nextInt(0x100) };
                final byte[] minKey = y == null || ByteUtil.compare(x, y) < 0 ? x : y;
                final byte[] maxKey = y == null || ByteUtil.compare(x, y) < 0 ? y : x;
                this.log.trace("[" + count + "]: REMOVE: [" + ByteUtil.toString(minKey) + ", " + ByteUtil.toString(maxKey) + ")");
                writes.setRemoves(writes.getRemoves().add(new KeyRange(minKey, maxKey)));
                if (maxKey == null)
                    map.tailMap(minKey, true).clear();
                else
                    map.subMap(minKey, true, maxKey, false).clear();
            }
        }
        return writes;
    }

    private TreeMap<byte[], byte[]> read(int count, KVStore kv) {
        return this.read(count, kv, ByteUtil.EMPTY, null);
    }

    private TreeMap<byte[], byte[]> read(int count, KVStore kv, byte[] minKey, byte[] maxKey) {
        final TreeMap<byte[], byte[]> map = new TreeMap<byte[], byte[]>(ByteUtil.COMPARATOR);
        this.log.trace("[" + count + "]: reading kv store");
        for (Iterator<KVPair> i = kv.getRange(minKey, maxKey, false); i.hasNext(); ) {
            final KVPair pair = i.next();
            map.put(pair.getKey(), pair.getValue());
        }
        return map;
    }

    private void compare(TreeMap<byte[], byte[]> map1, TreeMap<byte[], byte[]> map2) {
        final NavigableMap<String, String> smap1 = this.stringView(map1);
        final NavigableMap<String, String> smap2 = this.stringView(map2);
        Assert.assertEquals(smap1, smap2, "\n*** ACTUAL:\n" + smap1 + "\n*** EXPECTED:\n" + smap2 + "\n");
    }

    private NavigableMap<String, String> stringView(NavigableMap<byte[], byte[]> byteMap) {
        if (byteMap == null)
            return null;
        final Converter<String, byte[]> converter = ByteUtil.STRING_CONVERTER.reverse();
        return new ConvertedNavigableMap<String, String, byte[], byte[]>(byteMap, converter, converter);
    }
}

