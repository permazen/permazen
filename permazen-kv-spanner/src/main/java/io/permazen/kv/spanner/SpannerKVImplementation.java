
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.spanner;

import com.google.cloud.spanner.SpannerOptions;
import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class SpannerKVImplementation implements KVImplementation<SpannerKVImplementation.Config> {

    private OptionSpec<String> instanceOption;
    private OptionSpec<String> projectOption;
    private OptionSpec<String> databaseOption;
    private OptionSpec<String> tableOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.instanceOption == null, "duplicate option");
        Preconditions.checkState(this.projectOption == null, "duplicate option");
        Preconditions.checkState(this.databaseOption == null, "duplicate option");
        Preconditions.checkState(this.tableOption == null, "duplicate option");
        this.instanceOption = parser.accepts("spanner", "Specify Google Cloud Spanner instance ID")
          .withRequiredArg()
          .describedAs("instance-id");
        this.projectOption = parser.accepts("spanner-project", "Specify Google Cloud Spanner project ID")
          .availableIf(this.instanceOption)
          .withRequiredArg()
          .describedAs("project-id");
        this.databaseOption = parser.accepts("spanner-database",
            String.format("Specify Google Cloud Spanner database ID (default \"%s\")", SpannerKVDatabase.DEFAULT_DATABASE_ID))
          .availableIf(this.instanceOption)
          .withRequiredArg()
          .describedAs("database-id");
        this.tableOption = parser.accepts("spanner-table",
            String.format("Specify Google Cloud Spanner table name (default \"%s\")", SpannerKVDatabase.DEFAULT_TABLE_NAME))
          .availableIf(this.instanceOption)
          .withRequiredArg()
          .describedAs("table-name");
    }

    @Override
    public Config buildConfig(OptionSet options) {
        final String instanceId = this.instanceOption.value(options);
        if (instanceId == null)
            return null;
        final Config config = new Config(instanceId);
        config.setProjectId(this.projectOption.value(options));
        config.setDatabaseId(this.databaseOption.value(options));
        config.setTableName(this.tableOption.value(options));
        return config;
    }

    @Override
    public SpannerKVDatabase createKVDatabase(Config config, KVDatabase kvdb, AtomicKVStore kvstore) {
        final SpannerKVDatabase spannerKV = new SpannerKVDatabase();
        config.configure(spannerKV);
        return spannerKV;
    }

    @Override
    public String getDescription(Config config) {
        return "Spanner " + config.getInstanceId();
    }

// Options

    public static class Config {

        private final String instanceId;

        private String projectId;
        private String databaseId;
        private String tableName;

        public Config(String instanceId) {
            Preconditions.checkArgument(instanceId != null, "null instanceId");
            this.instanceId = instanceId;
        }

        public String getInstanceId() {
            return this.instanceId;
        }

        public String getProjectId() {
            return this.projectId;
        }
        public void setProjectId(String projectId) {
            this.projectId = projectId;
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

        public void configure(SpannerKVDatabase kvdb) {
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
