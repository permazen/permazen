
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.schema.SchemaModel;
import io.permazen.util.ImmutableNavigableMap;

import java.util.Map;
import java.util.TreeMap;

/**
 * Configuration for a {@link Transaction}.
 */
public final class TransactionConfig {

    private final SchemaModel schemaModel;
    private final int schemaVersion;
    private final boolean allowNewSchema;
    private final Map<String, ?> kvoptions;

    private TransactionConfig(Builder builder) {

        // Sanity checks
        Preconditions.checkArgument(builder.schemaVersion >= -1, "invalid schema version");
        Preconditions.checkArgument(builder.schemaModel != null || builder.schemaVersion >= 0,
          "auto-generating schema version but no schema model configured");

        // Copy, lock down, and validate schema model
        SchemaModel schemaModelCopy = null;
        if (builder.schemaModel != null) {
            schemaModelCopy = builder.schemaModel.isLockedDown() ? builder.schemaModel : builder.schemaModel.clone();
            schemaModelCopy.lockDown();
            schemaModelCopy.validate();
        }

        // Initialize
        this.schemaModel = schemaModelCopy;
        this.schemaVersion = builder.schemaVersion != -1 ? builder.schemaVersion : this.schemaModel.autogenerateVersion();
        this.allowNewSchema = builder.allowNewSchema;
        this.kvoptions = this.copyOptions(builder.kvoptions);
    }

    private <V> Map<String, V> copyOptions(Map<String, V> map) {
        return map != null ? new ImmutableNavigableMap<String, V>(new TreeMap<>(map)) : null;
    }

    /**
     * Get the schema model to use, or null to use a schema already recorded in the database.
     *
     * @return the schema to use during transactions
     * @see Builder#setSchemaModel
     */
    public SchemaModel getSchemaModel() {
        return this.schemaModel;
    }

    /**
     * Get the version number to use for the configured schema.
     *
     * @return schema version number, or zero to use the highest recorded version
     * @see Builder#setSchemaVersion
     */
    public int getSchemaVersion() {
        return this.schemaVersion;
    }

    /**
     * Get whether it is allowed to register a new schema model into the database.
     *
     * @return whether creating a new schema version is allowed
     * @see Builder#setAllowNewSchema
     */
    public boolean isAllowNewSchema() {
        return this.allowNewSchema;
    }

    /**
     * Configure {@link KVDatabase} transaction options.
     *
     * @return transaction options for the underlying {@link KVDatabase}, or null for none
     * @see Builder#setKVOptions
     * @see KVDatabase#createTransaction(Map)
     */
    public Map<String, ?> getKVOptions() {
        return this.kvoptions;
    }

    /**
     * Create a {@link Builder}.
     *
     * @return new transaction config builder
     */
    public static Builder builder() {
        return new Builder();
    }

// Builder

    public static final class Builder implements Cloneable {

        private SchemaModel schemaModel;
        private int schemaVersion = -1;
        private boolean allowNewSchema = true;
        private Map<String, ?> kvoptions;

    // Constructors

        private Builder() {
        }

        private Builder(TransactionConfig config) {
            this.schemaModel = config.schemaModel;
            this.schemaVersion = config.schemaVersion;
            this.allowNewSchema = config.allowNewSchema;
            this.kvoptions = config.kvoptions;
        }

    // Methods

        /**
         * Configure the schema to use.
         *
         * <p>
         * By default, this is null, which means the schema used will be one already registered in the database; in that case,
         * which one depends on the schema version, which must be explicitly {@linkplain #setSchemaVersion configured}.
         *
         * @param schemaModel schema to use when creating transactions
         * @return this instance
         */
        public Builder schemaModel(SchemaModel schemaModel) {
            this.schemaModel = schemaModel;
            return this;
        }

        /**
         * Configure the schema version number.
         *
         * <p>
         * By default, this is -1, which means a schema model must be {@linkplain #setSchemaModel configured} and
         * the schema version number will be {@linkplain SchemaModel#autogenerateVersion automatically generated} from it.
         *
         * <p>
         * If this is configured as zero, then the highest version schema already recorded in the database will be used.
         *
         * <p>
         * Otherwise, a positive value configures the schema version explicitly.
         *
         * @param schemaVersion a positive schema version number corresponding to the configured schema model,
         *   zero to use the highest version already recorded in the database, or -1 to generate a schema version automatically
         * @return this instance
         */
        public Builder schemaVersion(int schemaVersion) {
            Preconditions.checkArgument(schemaVersion >= -1, "invalid schema version");
            this.schemaVersion = schemaVersion;
            return this;
        }

        /**
         * Configure whether, if the configured schema is not already registered in the database
         * when new transactions are created, it is allowed to register it.
         *
         * <p>
         * The default value is true.
         *
         * @param allowNewSchema whether creating a new schema version is allowed
         * @return this instance
         */
        public Builder allowNewSchema(boolean allowNewSchema) {
            this.allowNewSchema = allowNewSchema;
            return this;
        }

        /**
         * Configure {@link KVDatabase} transaction options.
         *
         * <p>
         * The default value is null.
         *
         * @param kvoptions options to use when creating new transactions in the underlying {@link KVDatabase}, or null for none
         * @return this instance
         * @see KVDatabase#createTransaction(Map)
         */
        public Builder kvOptions(Map<String, ?> kvoptions) {
            this.kvoptions = kvoptions;
            return this;
        }

        /**
         * Create a new {@link TransactionConfig} from this instance.
         *
         * @return new transaction config
         */
        public TransactionConfig build() {
            return new TransactionConfig(this);
        }

        /**
         * Clone this instance.
         *
         * @return clone of this instance
         */
        public Builder clone() {
            try {
                return (Builder)super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
