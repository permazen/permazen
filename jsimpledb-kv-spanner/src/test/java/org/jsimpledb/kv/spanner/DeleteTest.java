
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.spanner;

import com.google.cloud.spanner.SpannerOptions;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.test.KVTestSupport;
import org.jsimpledb.util.ByteUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class DeleteTest extends KVTestSupport {

    private SpannerKVDatabase db;

    @BeforeClass(groups = "configure")
    @Parameters({ "spannerProject", "spannerInstance" })
    public void setSpannerConfig(@Optional String project, String instance) throws Exception {
        if (instance != null) {
            this.db = new SpannerKVDatabase();
            final SpannerOptions.Builder builder = SpannerOptions.newBuilder();
            if (project != null)
                builder.setProjectId(project);
            this.db.setSpannerOptions(builder.build());
            this.db.setInstanceId(instance);
        }
    }

    @Test
    protected void testDeleteToInfinity() throws Exception {
        this.db.start();
        try {

            final byte[] key1 = new byte[] { (byte)0x11 };
            final byte[] val1 = new byte[] { (byte)0xaa };
            final byte[] key2 = new byte[] { (byte)0x22 };
            final byte[] val2 = new byte[] { (byte)0xbb };

            // Populate DB
            KVTransaction tx = this.db.createTransaction();
            try {
                tx.removeRange(new byte[0], null);
                tx.put(key1, val1);
                tx.put(key2, val2);
                tx.commit();
            } finally {
                tx.rollback();
            }

            // Display db
            this.show("initial content");

            // Delete to infinity
            tx = this.db.createTransaction();
            try {
                tx.removeRange(ByteUtil.getNextKey(key1), null);
                tx.commit();
            } finally {
                tx.rollback();
            }

            // Verify second key is gone
            tx = this.db.createTransaction();
            try {
                this.show("after remove");
                Assert.assertNull(tx.get(key2));
                tx.commit();
            } finally {
                tx.rollback();
            }

        } finally {
            this.db.stop();
        }
    }

    private void show(String label) throws Exception {
        this.log.info("Spanner content (" + label + "):");
        KVTransaction tx = this.db.createTransaction();
        try {
            this.show(tx, label);
            tx.commit();
        } finally {
            tx.rollback();
        }
    }

    private void show(KVTransaction tx, String label) throws Exception {
        final Exception e = this.showKV(tx, label);
        if (e != null)
            throw e;
    }
}

