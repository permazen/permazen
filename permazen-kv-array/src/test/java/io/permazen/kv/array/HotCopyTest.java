
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.array;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;

import io.permazen.kv.KVPair;
import io.permazen.test.TestSupport;
import io.permazen.util.ByteUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HotCopyTest extends TestSupport {

    @Test
    private void testHotCopy() throws Exception {

        // Create persistent k/v store
        final File mainDir = this.createTempDirectory();
        AtomicArrayKVStore kv = new AtomicArrayKVStore();
        kv.setDirectory(mainDir);
        kv.start();

        // Populate it
        kv.put("aaa".getBytes(), "asflksjfljaksdadf".getBytes());
        kv.put("bbb".getBytes(), "7jsdj".getBytes());
        kv.put("ccc".getBytes(), "hsd8373w8djl".getBytes());
        kv.put("ddd".getBytes(), "8888888888888888888888888888888888888888888888888888888".getBytes());
        kv.put("eee".getBytes(), ByteUtil.EMPTY);

        // Perform compaction
        kv.scheduleCompaction().get();

        // Perform more mutations
        kv.put("aaa".getBytes(), "aaaaa22222".getBytes());
        kv.remove("bbb".getBytes());
        kv.put("ddd".getBytes(), "fweep".getBytes());
        kv.put("fff".getBytes(), "blander".getBytes());

        // Check bogus hot copy
        try {
            kv.hotCopy(mainDir);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Create hot copy
        final File backupDir = this.createTempDirectory();
        kv.hotCopy(backupDir);

        // Perform even more mutations
        kv.put(ByteUtil.EMPTY, "empty".getBytes());
        kv.remove("aaa".getBytes());
        kv.put("bbb".getBytes(), "nyahnyah".getBytes());
        kv.remove("fff".getBytes());

        // Shutdown original k/v store
        this.log.info("shutting down original k/v store");
        kv.scheduleCompaction();
        kv.stop();

        // Delete original directory
        this.deleteDirectoryHierarchy(mainDir);

        // Create new k/v store from backup copy
        kv = new AtomicArrayKVStore();
        kv.setDirectory(backupDir);
        kv.start();

        // Get expected contents
        final HashMap<String, String> expected = new HashMap<>();
        expected.put("aaa", "aaaaa22222");
        expected.put("ccc", "hsd8373w8djl");
        expected.put("ddd", "fweep");
        expected.put("eee", "");
        expected.put("fff", "blander");

        // Get actual contents
        final HashMap<String, String> actual = new HashMap<>();
        for (Iterator<KVPair> i = kv.getRange(null, null, false); i.hasNext(); ) {
            final KVPair pair = i.next();
            actual.put(new String(pair.getKey()), new String(pair.getValue()));
        }

        // Verify contents
        Assert.assertEquals(actual, expected);

        // Shutdown backup k/v store
        this.log.info("shutting down backup k/v store");
        kv.scheduleCompaction();
        kv.stop();

        // Delete backup directory
        this.deleteDirectoryHierarchy(backupDir);
    }
}

