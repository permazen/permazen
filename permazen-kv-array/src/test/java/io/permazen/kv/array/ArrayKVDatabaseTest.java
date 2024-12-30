
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.test.KVDatabaseTest;
import io.permazen.util.ByteData;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class ArrayKVDatabaseTest extends KVDatabaseTest {

    private static final int NUM_BIGWRITER_THREADS = 7;

    private ArrayKVDatabase arrayKV;

    @BeforeClass(groups = "configure")
    @Parameters({
      "arrayDirPrefix",
      "arrayCompactMaxDelay",
      "arrayCompactSpaceLowWater",
      "arrayCompactSpaceHighWater",
    })
    public void setArrayDirPrefix(@Optional String arrayDirPrefix,
      @Optional("90") int compactMaxDelay,
      @Optional("65536") int compactLowWater,
      @Optional("1073741824") int compactHighWater) throws IOException {
        if (arrayDirPrefix != null) {
            final File dir = File.createTempFile(arrayDirPrefix, null);
            Assert.assertTrue(dir.delete());
            Assert.assertTrue(dir.mkdirs());
            dir.deleteOnExit();
            final AtomicArrayKVStore kvstore = new AtomicArrayKVStore();
            kvstore.setDirectory(dir);
            kvstore.setCompactMaxDelay(compactMaxDelay);
            kvstore.setCompactLowWater(compactLowWater);
            kvstore.setCompactHighWater(compactHighWater);
            this.arrayKV = new ArrayKVDatabase();
            this.arrayKV.setKVStore(kvstore);
        }
    }

    @AfterClass
    public void reportTotalMillisWaiting() {
        if (this.arrayKV != null)
            this.log.info("Total delay for compaction: {}ms", this.arrayKV.getKVStore().getTotalMillisWaiting());
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.arrayKV;
    }

    @Test
    private void testLotsOfData() throws Exception {
        final BigWriter[] threads = new BigWriter[NUM_BIGWRITER_THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new BigWriter(this.arrayKV);
            threads[i].start();
        }
        for (Thread thread : threads)
            thread.join();
    }

    private static class BigWriter extends Thread {

        private static final int MAX_VALUE_LENGTH = 1024 * 1024;
        private static final int NUM_ITERATIONS_PER_THREAD = 100;
        private static final int NUM_KEYS = 37;

        private final ArrayKVDatabase kvdb;
        private final Random random = new Random(this.getId());

        BigWriter(ArrayKVDatabase kvdb) {
            this.kvdb = kvdb;
        }

        @Override
        public void run() {
            for (int i = 0; i < NUM_ITERATIONS_PER_THREAD; i++) {
                final ArrayKVTransaction tx = this.kvdb.createTransaction();
                boolean success = false;
                try {
                    final ByteData key = ByteData.of((byte)this.random.nextInt(NUM_KEYS));
                    final ByteData prevValue = tx.get(key);
                    if (prevValue == null) {
                        final byte[] nextValueBytes = new byte[this.random.nextInt(MAX_VALUE_LENGTH - 1) + 1];
                        this.random.nextBytes(nextValueBytes);
                        tx.put(key, ByteData.of(nextValueBytes));
                    } else if (this.random.nextInt(5) == 3)
                        tx.remove(key);
                    else {
                        final byte[] nextValueBytes = prevValue.toByteArray();
                        nextValueBytes[this.random.nextInt(nextValueBytes.length)] ^= (byte)this.random.nextInt();
                        tx.put(key, ByteData.of(nextValueBytes));
                    }
                    tx.commit();
                    success = true;
                } catch (RetryKVTransactionException e) {
                    // ignore
                } finally {
                    if (!success)
                        tx.rollback();
                }
            }
        }
    }
}
