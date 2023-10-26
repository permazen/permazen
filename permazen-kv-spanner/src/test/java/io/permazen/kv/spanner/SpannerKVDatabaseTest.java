
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.spanner;

import com.google.cloud.spanner.SpannerOptions;

import io.permazen.kv.test.KVDatabaseTest;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class SpannerKVDatabaseTest extends KVDatabaseTest {

    private SpannerKVDatabase spannerKV;

    @BeforeClass(groups = "configure")
    @Parameters({ "spannerProject", "spannerInstance" })
    public void setSpannerConfig(@Optional String project, @Optional String instance) throws Exception {
        if (instance != null) {
            this.spannerKV = new SpannerKVDatabase();
            final SpannerOptions.Builder builder = SpannerOptions.newBuilder();
            if (project != null)
                builder.setProjectId(project);
            this.spannerKV.setSpannerOptions(builder.build());
            this.spannerKV.setInstanceId(instance);
        }
    }

    @Override
    protected SpannerKVDatabase getKVDatabase() {
        return this.spannerKV;
    }

    @Override
    protected int getParallelTransactionLoopCount() {
        return 7;
    }

    @Override
    protected int getParallelTransactionTaskCount() {
        return 7;
    }

    @Override
    protected int getSequentialTransactionLoopCount() {
        return 13;
    }

    @Override
    protected int getRandomTaskMaxIterations() {
        return 25;
    }

    @Override
    protected boolean supportsReadOnlyAfterDataAccess() {
        return false;
    }
}
