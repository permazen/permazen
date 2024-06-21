
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.util.ImmutableNavigableMap;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.BiPredicate;

/**
 * Configuration for a {@link Transaction}.
 */
public final class TransactionConfig {

    private final SchemaModel schemaModel;
    private final boolean allowNewSchema;
    private final SchemaRemoval schemaRemoval;
    private final Map<String, ?> kvoptions;

    private TransactionConfig(Builder builder) {

        // Use an empty schema if none specified
        this.schemaModel = Optional.ofNullable(builder.schemaModel)
          .map(SchemaModel::clone)
          .orElseGet(SchemaModel::new);

        // Lock down schema model
        this.schemaModel.lockDown(true);

        // Validate schema model
        this.schemaModel.validate();

        // Initialize other stuff
        this.allowNewSchema = builder.allowNewSchema;
        this.schemaRemoval = builder.schemaRemoval;
        this.kvoptions = this.copyOptions(builder.kvoptions);
    }

    private <V> Map<String, V> copyOptions(Map<String, V> map) {
        return map != null ? new ImmutableNavigableMap<String, V>(new TreeMap<>(map)) : null;
    }

    /**
     * Get the schema model to use.
     *
     * @return the schema to use during transactions
     * @see Builder#schemaModel
     */
    public SchemaModel getSchemaModel() {
        return this.schemaModel;
    }

    /**
     * Get whether it is allowed to register a new schema model into the database.
     *
     * @return whether registering a new schema is allowed
     * @see Builder#allowNewSchema
     */
    public boolean isAllowNewSchema() {
        return this.allowNewSchema;
    }

    /**
     * Get when to automatically detect and remove unused schemas.
     *
     * @return if and when to remove unused schemas
     * @see Builder#schemaRemoval
     */
    public SchemaRemoval getSchemaRemoval() {
        return this.schemaRemoval;
    }

    /**
     * Configure {@link KVDatabase} transaction options.
     *
     * @return transaction options for the underlying {@link KVDatabase}, or null for none
     * @see Builder#kvOptions
     * @see KVDatabase#createTransaction(Map)
     */
    public Map<String, ?> getKVOptions() {
        return this.kvoptions;
    }

// Other Methods

    /**
     * Create a {@link Builder}.
     *
     * @return new transaction config builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a {@link Builder} that is pre-configured as a copy of this instance.
     *
     * @return new pre-configured transaction config builder
     */
    public Builder copy() {
        return new Builder()
          .schemaModel(this.schemaModel)
          .allowNewSchema(this.allowNewSchema)
          .schemaRemoval(this.schemaRemoval)
          .kvOptions(this.kvoptions);
    }

    /**
     * Convenience method that uses this instance to create a new transaction in the given database.
     *
     * @param db database in which to open a new transaction
     * @return new transaction in {@code db}
     * @throws IllegalArgumentException if {@code db} is null
     * @see Database#createTransaction(TransactionConfig)
     */
    public Transaction newTransaction(Database db) {
        Preconditions.checkArgument(db != null, "null db");
        return db.createTransaction(this);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[schema=" + this.schemaModel.getSchemaId()
          + ",allowNewSchema=" + this.allowNewSchema
          + ",schemaRemoval=" + this.schemaRemoval
          + (this.kvoptions != null ? ",kvoptions=" + this.kvoptions : "")
          + "]";
    }

// Builder

    /**
     * Builder for {@link TransactionConfig}s.
     */
    public static final class Builder implements Cloneable {

        private SchemaModel schemaModel;
        private boolean allowNewSchema = true;
        private SchemaRemoval schemaRemoval = SchemaRemoval.CONFIG_CHANGE;
        private Map<String, ?> kvoptions;

    // Constructors

        private Builder() {
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
         * Configure when to automatically detect and remove unused schemas from the database.
         *
         * <p>
         * When schema removal is enabled, unused schemas (i.e., those for which zero objects exist) are automatically detected
         * and removed at the beginning certain transactions. This operation adds a small amount of overhead.
         *
         * <p>
         * The default value is {@link SchemaRemoval#CONFIG_CHANGE}.
         *
         * @param schemaRemoval if and when to automatically detect and remove unused schemas
         * @return this instance
         * @throws IllegalArgumentException if {@code schemaRemoval} is null
         */
        public Builder schemaRemoval(SchemaRemoval schemaRemoval) {
            Preconditions.checkArgument(schemaRemoval != null, "null schemaRemoval");
            this.schemaRemoval = schemaRemoval;
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

// SchemaRemoval

    /**
     * Configures if and when to automatically detect and remove unused schemas from a database.
     *
     * <p>
     * When enabled, schema removal occurs at the beginning of a transaction.
     *
     * @see TransactionConfig.Builder#schemaRemoval
     */
    public enum SchemaRemoval {

        /**
         * Disable automatic schema removal.
         */
        NEVER((firstTransaction, newConfig) -> false),

        /**
         * Perform automatic schema removal only at the start of the very first transaction.
         */
        ONCE((firstTransaction, newConfig) -> firstTransaction),

        /**
         * Perform automatic schema removal at the start of the very first transaction, and any other transaction
         * configured differently from the previous transaction.
         */
        CONFIG_CHANGE((firstTransaction, newConfig) -> firstTransaction || newConfig),

        /**
         * Perform automatic schema removal at the start of every transaction.
         */
        ALWAYS((firstTransaction, newConfig) -> true);

        private final BiPredicate<Boolean, Boolean> tester;

        SchemaRemoval(BiPredicate<Boolean, Boolean> tester) {
            this.tester = tester;
        }

        public boolean shouldRemove(boolean firstTransaction, boolean newConfig) {
            return this.tester.test(firstTransaction, newConfig);
        }
    }
}
