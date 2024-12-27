
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import io.permazen.kv.KVPair;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.TestKVDatabase;
import io.permazen.test.TestSupport;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;

import java.util.ArrayList;
import java.util.function.Consumer;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BranchedKVTransactionTest extends TestSupport {

    private static final ByteData KEY_00 = ByteData.of(0x00);
    private static final ByteData KEY_01 = ByteData.of(0x01);
    private static final ByteData KEY_02 = ByteData.of(0x02);
    private static final ByteData KEY_03 = ByteData.of(0x03);
    private static final ByteData KEY_04 = ByteData.of(0x04);
    private static final ByteData KEY_05 = ByteData.of(0x05);
    private static final ByteData KEY_06 = ByteData.of(0x06);
    private static final ByteData KEY_07 = ByteData.of(0x07);

    private static final ByteData VAL_01 = ByteData.of(0x01);
    private static final ByteData VAL_02 = ByteData.of(0x02);
    private static final ByteData VAL_03 = ByteData.of(0x03);
    private static final ByteData VAL_04 = ByteData.of(0x04);
    private static final ByteData VAL_05 = ByteData.of(0x05);
    private static final ByteData VAL_06 = ByteData.of(0x06);
    private static final ByteData VAL_07 = ByteData.of(0x07);

// VIEW

    @Test
    public void testBranchedKVTransaction() throws Exception {

        final TestKVDatabase kvdb = new TestKVDatabase();

        // Initialize database
        KVTransaction tx = kvdb.createTransaction();
        tx.put(KEY_01, VAL_01);
        tx.put(KEY_02, VAL_02);
        tx.put(KEY_03, VAL_03);
        tx.put(KEY_05, VAL_05);
        tx.put(KEY_06, VAL_06);
        tx.put(KEY_07, VAL_07);
        tx.commit();

        // Sanity check expected errors in INITIAL state
        final ArrayList<Consumer<BranchedKVTransaction>> list = new ArrayList<>();
        list.add(btx -> btx.get(KEY_01));
        list.add(btx -> btx.remove(KEY_02));
        list.add(BranchedKVTransaction::commit);
        list.add(BranchedKVTransaction::readOnlySnapshot);
        for (Consumer<BranchedKVTransaction> c : list) {

            // Before open() it should fail
            final BranchedKVTransaction btx = new BranchedKVTransaction(kvdb);
            try {
                c.accept(btx);
                assert false : "expected failure";
            } catch (IllegalStateException e) {
                this.log.debug("got expected {}", e.toString());
            }

            // Do open()
            btx.open();

            // After open() it should succeed
            c.accept(btx);

            // Cleanup
            btx.close();
        }

        // Create and open a branched tx
        BranchedKVTransaction btx = new BranchedKVTransaction(kvdb);
        btx.open();

        // Do some non-conflicting reads in the branched transaction
        btx.get(KEY_01);
        try (CloseableIterator<KVPair> i = btx.getRange(KEY_03, KEY_06)) {
            while (i.hasNext())
                i.next();
        }

        // Do some writes in the branched transaction
        btx.remove(KEY_01);
        btx.remove(KEY_07);

        // Make some modifications to the database behind the scenes
        kvdb.getKVStore().remove(KEY_02);
        kvdb.getKVStore().put(KEY_06, VAL_01);

        // Commit should succeed
        btx.commit();

        // Verify changes were merged
        Assert.assertNull(kvdb.getKVStore().get(KEY_01));
        Assert.assertNull(kvdb.getKVStore().get(KEY_02));
        Assert.assertEquals(kvdb.getKVStore().get(KEY_03), VAL_03);
        Assert.assertEquals(kvdb.getKVStore().get(KEY_06), VAL_01);
        Assert.assertNull(kvdb.getKVStore().get(KEY_07));

        // Create another branched tx
        btx = new BranchedKVTransaction(kvdb);
        btx.open();

        // Do more reads & writes in the branched transaction
        try (CloseableIterator<KVPair> i = btx.getRange(KEY_04, KEY_06)) {
            while (i.hasNext())
                i.next();
        }
        btx.remove(KEY_06);

        // Now make conflicting modifications to the database behind the scenes
        kvdb.getKVStore().put(KEY_05, VAL_03);

        // Now commit should fail
        try {
            btx.commit();
            assert false : "expected failure";
        } catch (TransactionConflictException e) {
            this.log.debug("got expected {}", e.toString());
        }

        // Database should not be affected by failed commit
        Assert.assertEquals(kvdb.getKVStore().get(KEY_05), VAL_03);
    }
}
