
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.spanner;

import com.google.cloud.spanner.SpannerOptions;

import io.permazen.kv.test.KVTestSupport;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class DeleteTest extends KVTestSupport {

    private SpannerKVDatabase db;

    @BeforeClass(groups = "configure")
    @Parameters({ "spannerProject", "spannerInstance" })
    public void setSpannerConfig(@Optional String project, @Optional String instance) throws Exception {
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
        if (this.db == null)
            return;
        this.db.start();
        try {

            final ByteData key1 = ByteData.of(0x11);
            final ByteData val1 = ByteData.of(0xaa);
            final ByteData key2 = ByteData.of(0x22);
            final ByteData val2 = ByteData.of(0xbb);

            // Populate DB
            this.tryNtimes(this.db, tx -> {
                tx.removeRange(ByteData.empty(), null);
                tx.put(key1, val1);
                tx.put(key2, val2);
            });

            // Display db
            this.show("initial content");

            // Delete to infinity
            this.tryNtimes(this.db, tx -> tx.removeRange(ByteUtil.getNextKey(key1), null));

            // Verify second key is gone
            this.tryNtimes(this.db, tx -> {
                this.show("after remove");
                Assert.assertNull(tx.get(key2));
            });

        } finally {
            this.db.stop();
        }
    }

    private void show(String label) {
        this.tryNtimes(this.db, tx -> {
            final RuntimeException e = this.showKV(tx, "Spanner content (" + label + "):");
            if (e != null)
                throw e;
        });
    }
}
