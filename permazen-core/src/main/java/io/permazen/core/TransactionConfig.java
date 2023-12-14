
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.schema.SchemaModel;
import io.permazen.util.ImmutableNavigableMap;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Configuration for a {@link Transaction}.
 */
public final class TransactionConfig {

    private final SchemaModel schemaModel;
    private final boolean allowNewSchema;
    private final boolean garbageCollectSchemas;
    private final Map<String, ?> kvoptions;

    private TransactionConfig(Builder builder) {

        // Use an empty schema if none specified
        this.schemaModel = Optional.ofNullable(builder.schemaModel)
          .map(SchemaModel::clone)
          .orElseGet(SchemaModel::new);

        // Lock down schema model
        this.schemaModel.lockDown();

        // Validate schema model
        this.schemaModel.validate();

        // Initialize other stuff
        this.allowNewSchema = builder.allowNewSchema;
        this.garbageCollectSchemas = builder.garbageCollectSchemas;
        this.kvoptions = this.copyOptions(builder.kvoptions);
    }

    private <V> Map<String, V> copyOptions(Map<String, V> map) {
        return map != null ? new ImmutableNavigableMap<String, V>(new TreeMap<>(map)) : null;
    }

    /**
     * Get the schema model to use.
     *
     * @return the schema to use during transactions
     * @see Builder#setSchemaModel
     */
    public SchemaModel getSchemaModel() {
        return this.schemaModel;
    }

    /**
     * Get whether it is allowed to register a new schema model into the database.
     *
     * @return whether registering a new schema is allowed
     * @see Builder#setAllowNewSchema
     */
    public boolean isAllowNewSchema() {
        return this.allowNewSchema;
    }

    /**
     * Configure whether automatic schema garbage collection should be enabled.
     *
     * @return whether automatically garbage collect obsolete schemas
     * @see Builder#setGarbageCollectSchemas
     */
    public boolean isGarbageCollectSchemas() {
        return this.garbageCollectSchemas;
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
        private boolean allowNewSchema = true;
        private boolean garbageCollectSchemas;
        private Map<String, ?> kvoptions;

    // Constructors

        private Builder() {
        }

        private Builder(TransactionConfig config) {
            this.schemaModel = config.schemaModel;
            this.allowNewSchema = config.allowNewSchema;
            this.garbageCollectSchemas = config.garbageCollectSchemas;
            this.kvoptions = config.kvoptions;
        }

    // Methods

        /**
         * Configure the schema to use.
         *
         * <p>
         * By default this is null, which means that an empty schema will be used.
         *
         * @param schemaModel schema to use when creating transactions
         * @return this instance
         */
        public Builder schemaModel(SchemaModel schemaModel) {
            this.schemaModel = schemaModel;
            return this;
        }

        /**
         * Configure whether, if the configured schema is not already registered in the database
         * when new transactions are created, it is allowed to register it.
         *
         * <p>
         * The default value is true.
         *
         * @param allowNewSchema whether registering a new schema is allowed
         * @return this instance
         */
        public Builder allowNewSchema(boolean allowNewSchema) {
            this.allowNewSchema = allowNewSchema;
            return this;
        }

        /**
         * Configure whether automatic schema garbage collection should be enabled.
         *
         * <p>
         * When automatic schema garbage collection is enabled, unused schemas are garbage collected
         * at the start of each transaction. This adds a small amount of overhead to transaction startup,
         * but only when a new schema set is encountered.
         *
         * <p>
         * The default value is false.
         *
         * @param garbageCollectSchemas whether to enable automatic schema garbage collection
         * @return this instance
         */
        public Builder garbageCollectSchemas(boolean garbageCollectSchemas) {
            this.garbageCollectSchemas = garbageCollectSchemas;
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
         * @throws InvalidSchemaException if the {@linkplain #schemaModel configured schema}
         *  is invalid (i.e., does not pass validation checks)
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
