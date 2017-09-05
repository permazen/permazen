
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.spanner;

import com.google.cloud.spanner.SpannerOptions;
import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.util.ArrayDeque;

public class SpannerKVImplementation extends KVImplementation {

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--spanner id",           "Specify Google Cloud Spanner instance ID" },
            { "--spanner-project id",   "Specify Google Cloud Spanner project ID" },
            { "--spanner-database id",
                "Specify Google Cloud Spanner database ID (default `" + SpannerKVDatabase.DEFAULT_DATABASE_ID + "')" },
            { "--spanner-table name",
                "Specify Google Cloud Spanner table name (default `" + SpannerKVDatabase.DEFAULT_TABLE_NAME + "')" },
        };
    }

    @Override
    public Config parseCommandLineOptions(ArrayDeque<String> options) {
        final Config config = new Config();
        config.setInstanceId(this.parseCommandLineOption(options, "--spanner"));
        config.setProjectId(this.parseCommandLineOption(options, "--spanner-project"));
        config.setDatabaseId(this.parseCommandLineOption(options, "--spanner-database"));
        config.setTableName(this.parseCommandLineOption(options, "--spanner-table"));
        return !config.isEmpty() ? config : null;
    }

    @Override
    public SpannerKVDatabase createKVDatabase(Object configuration, KVDatabase kvdb, AtomicKVStore kvstore) {
        final Config config = (Config)configuration;
        final SpannerKVDatabase spannerKV = new SpannerKVDatabase();
        config.configure(spannerKV);
        return spannerKV;
    }

    @Override
    public String getDescription(Object configuration) {
        final Config config = (Config)configuration;
        return "Spanner " + config.getInstanceId();
    }

// Options

    private static class Config {

        private String projectId;
        private String instanceId;
        private String databaseId;
        private String tableName;

        public String getProjectId() {
            return this.projectId;
        }
        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getInstanceId() {
            return this.instanceId;
        }
        public void setInstanceId(String instanceId) {
            this.instanceId = instanceId;
        }

        public String getDatabaseId() {
            return this.databaseId;
        }
        public void setDatabaseId(String databaseId) {
            this.databaseId = databaseId;
        }

        public String getTableName() {
            return this.tableName;
        }
        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public boolean isEmpty() {
            return this.projectId == null && this.instanceId == null && this.databaseId == null && this.tableName == null;
        }

        public void configure(SpannerKVDatabase kvdb) {
            Preconditions.checkArgument(this.instanceId != null, "Spanner instance ID must be specified via the `--spanner' flag");
            final SpannerOptions.Builder builder = SpannerOptions.newBuilder();
            if (this.projectId != null)
                builder.setProjectId(this.projectId);
            kvdb.setSpannerOptions(builder.build());
            kvdb.setInstanceId(this.instanceId);
            if (this.databaseId != null)
                kvdb.setDatabaseId(this.databaseId);
            if (this.tableName != null)
                kvdb.setTableName(this.tableName);
        }
    }
}
