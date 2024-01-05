
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.DefaultEncodingRegistry;
import io.permazen.encoding.Encoding;
import io.permazen.encoding.EncodingRegistry;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.KeyRange;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.NavigableSets;
import io.permazen.util.UnsignedIntEncoder;

import java.io.Closeable;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides an object database abstraction on top of a key/value database.
 *
 * <p>
 * Includes support for:
 * <ul>
 *  <li>Objects and fields defined by a {@link SchemaModel}, with positive schema verification</li>
 *  <li>Simple value fields containing any standard or custom type that has an {@link Encoding}</li>
 *  <li>Invertable reference fields with referential integrity and configurable delete cascading</li>
 *  <li>Lockless counter fields</li>
 *  <li>Complex fields of type {@link java.util.List}, {@link java.util.NavigableSet}, and {@link java.util.NavigableMap}</li>
 *  <li>Configurable indexing of any simple field or complex sub-field</li>
 *  <li>Composite indexes on multiple simple fields</li>
 *  <li>Notification of object creation and deletion</li>
 *  <li>Notification of object field changes, as seen through an arbitrary path of references</li>
 *  <li>Automatic schema tracking and migration with notification support</li>
 * </ul>
 *
 * <p>
 * See {@link Transaction} for further details on the above functionality.
 *
 * <p>
 * This class defines an abstraction layer that usually sits below a {@link io.permazen.Permazen} but is completely
 * independent of {@link io.permazen.Permazen} and can be used on its own.
 * Compared to {@link io.permazen.Permazen}, a {@link Database} has these differences:
 * <ul>
 *  <li>A {@link SchemaModel} must be explicitly provided to define the schema in use, whereas when using a
 *      {@link io.permazen.Permazen} the schema is derived automatically from annotated Java model classes.</li>
 *  <li>Object references are represented by {@link ObjId}s instead of Java objects</li>
 *  <li>There is no explicit notion of object sub-types, i.e., the object type hierarchy is completely flat.
 *      However, object types can share the same field definitions, and reference fields can be restricted
 *      to only refer to certain other types.</li>
 *  <li>Enum values are represented by {@link EnumValue} objects.</li>
 *  <li>There is no automatic validation support.</li>
 * </ul>
 *
 * <p>
 * <b>Schema Tracking</b>
 *
 * <p>
 * Within each {@link Database} is stored a record of all schemas used with that database (termed the "schema bundle").
 * When creating a new transaction, the caller provides a {@link SchemaModel} to use for accessing objects in the transaction.
 * If the schema is not already registered in the database, it is automatically registered if at the start of the transaction
 * assuming {@link TransactionConfig#isAllowNewSchema} is true (otherwise a {@link SchemaMismatchException} is thrown).
 *
 * <p>
 * <b>Schema Migration</b>
 *
 * <p>
 * Each object in a {@link Database} is associated with an {@link ObjType object type} in one of the registered schemas;
 * this dictates which fields the object contains, how they are indexed, etc. Newly created objects are always defined
 * in terms of the transaction's configured schema, but it's also possible to access objects that were created
 * in previous transactions under different schemas.
 *
 * <p>
 * When an object is accessed during a {@link Transaction}, if the object's schema is not the transaction's configured
 * schema, then (if requested) the object will be automatically migrated to the transaction's schema if the object's type
 * still exists (otherwise a {@link TypeNotInSchemaException} is thrown).
 *
 * <p>
 * Object migration involves the following steps:
 * <ul>
 *  <li>Fields that exist in the old schema but not in the current schema are removed.</li>
 *  <li>Fields that exist in the current schema but not in the old schema are initialized to their default values.</li>
 *  <li>For fields that are common to both the old and the new schema:</li>
 *  <ul>
 *      <li>If the two field's {@link Encoding}s are the same, the value is not changed
 *      <li>Otherwise, the field's old value is converted to the new {@link Encoding}, if possible,
 *          via {@link Encoding#convert Encoding.convert()}.
 *      <li>Otherwise, the field is reset to its default value.
 *  </ul>
 *  <li>Any {@link SchemaChangeListener}s registered with the {@link Transaction} are notified.</li>
 * </ul>
 *
 * <p>
 * Object type and field identity is based on names. So when comparing across schemas,
 * two object types or fields are assumed to be "the same" if they have the same name.
 *
 * <p>
 * Indexes may be added and removed across different schemas without losing information, however,
 * indexes only contain objects whose schema defines the index. In other words, it's not
 * possible to find an object using an index that was added after the object was created, at least
 * not until the object is migrated to a newer schema. Similarly for other schema-defined behavior,
 * for example, the {@link DeleteAction} taken when a referenced object is deleted depends on the
 * {@link DeleteAction} configured in the schema of the object containing the reference.
 *
 * @see Transaction
 * @see io.permazen
 */
public class Database {

    /**
     * The maximum number of fields that may be indexed in a composite index ({@value #MAX_INDEXED_FIELDS}).
     */
    // COMPOSITE-INDEX
    public static final int MAX_INDEXED_FIELDS = 4;

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final KVDatabase kvdb;

    @GuardedBy("this")
    private EncodingRegistry encodingRegistry;
    @GuardedBy("this")
    private SchemaCache schemaCache;

    /**
     * Constructor.
     *
     * @param kvdb the underlying key/value store in which to store information
     * @throws IllegalArgumentException if {@code kvdb} is null
     */
    public Database(KVDatabase kvdb) {
        Preconditions.checkArgument(kvdb != null, "null kvdb");
        this.kvdb = kvdb;
        this.encodingRegistry = new DefaultEncodingRegistry();
    }

    /**
     * Get the {@link EncodingRegistry} associated with this instance.
     *
     * <p>
     * By default a {@link DefaultEncodingRegistry} is configured.
     *
     * @param encodingRegistry encoding registry to associate with this instance
     * @throws IllegalArgumentException if {@code encodingRegistry} is null
     */
    public synchronized void setEncodingRegistry(EncodingRegistry encodingRegistry) {
        this.encodingRegistry = encodingRegistry;
    }

    /**
     * Get the {@link EncodingRegistry} associated with this instance.
     *
     * @return encoding registry associated with this instance
     */
    public synchronized EncodingRegistry getEncodingRegistry() {
        return this.encodingRegistry;
    }

    /**
     * Get the {@link KVDatabase} underlying this instance.
     *
     * @return underlying key/value database
     */
    public KVDatabase getKVDatabase() {
        return this.kvdb;
    }

    /**
     * Create a new {@link Transaction} on this database using the given {@link SchemaModel}
     * and the defaults for all other {@link TransactionConfig} items.
     *
     * <p>
     * Equivalent to: {@link #createTransaction(TransactionConfig) createTransaction}{@code
     *  (TransactionConfig.builder().schemaModel(schemaModel).build())}.
     *
     * @param schemaModel schema model
     * @return newly created transaction
     * @throws InvalidSchemaException if the configured schema is invalid (i.e., does not pass validation checks)
     * @throws SchemaMismatchException if the configured {@link SchemaModel} has any explicit storage ID assignments
     *  that conflict with other schemas already recorded in the database
     * @throws InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws InconsistentDatabaseException if an uninitialized database is encountered but the database is not empty
     * @throws IllegalArgumentException if {@code schemaModel} is null
     */
    public Transaction createTransaction(SchemaModel schemaModel) {
        Preconditions.checkArgument(schemaModel != null, "null schemaModel");
        return this.createTransaction(TransactionConfig.builder().schemaModel(schemaModel).build());
    }

    /**
     * Create a new {@link Transaction} on this database using the specific configuration.
     *
     * @param txConfig transaction configuration
     * @return newly created transaction
     * @throws InvalidSchemaException if the configured schema is invalid (i.e., does not pass validation checks)
     * @throws SchemaMismatchException if the configured schema is not registered in the database and
     *  {@link TransactionConfig#isAllowNewSchema} is false
     * @throws SchemaMismatchException if the configured {@link SchemaModel} has any explicit storage ID assignments
     *  that conflict with other schemas already recorded in the database
     * @throws InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws InconsistentDatabaseException if an uninitialized database is encountered but the database is not empty
     * @throws IllegalArgumentException if {@code txConfig} is null
     */
    public Transaction createTransaction(TransactionConfig txConfig) {
        Preconditions.checkArgument(txConfig != null, "null txConfig");
        final KVTransaction kvt = this.kvdb.createTransaction(txConfig.getKVOptions());
        boolean success = false;
        try {
            final Transaction tx = this.createTransaction(kvt, txConfig);
            success = true;
            return tx;
        } finally {
            if (!success) {
                try {
                    kvt.rollback();
                } catch (KVTransactionException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Create a new {@link Transaction} on this database using an already-opened {@link KVTransaction} and specified
     * configuration.
     *
     * <p>
     * The given {@link KVTransaction} will be committed or rolled-back along with the returned {@link Transaction}.
     *
     * <p>
     * See {@link #createTransaction(TransactionConfig)} for further details.
     *
     * @param kvt already opened key/value store transaction
     * @param txConfig transaction configuration
     * @return newly created transaction
     * @throws IllegalArgumentException if {@code kvt} or {@code txConfig} is null
     */
    public Transaction createTransaction(KVTransaction kvt, TransactionConfig txConfig) {

        // Validate meta-data
        final Schema schema = this.verifySchemaBundle(kvt, txConfig);

        // Create transaction
        final Transaction tx = new Transaction(this, kvt, schema);

        // Synchronize with all future synchronized transaction access
        synchronized (tx) {
            return tx;
        }
    }

    /**
     * Create a detached transaction based on the provided key/value store.
     *
     * <p>
     * The key/value store will be initialized if necessary (i.e., {@code kvstore} may be empty), otherwise it will be
     * validated against the schema information associated with this instance.
     *
     * <p>
     * The returned {@link DetachedTransaction} does not support {@link DetachedTransaction#commit commit()} or
     * {@link DetachedTransaction#rollback rollback()} and can be used indefinitely.
     * It does support {@link DetachedTransaction#addCallback addCallback()} but that method has no effect because
     * the transaction cannot be committed.
     *
     * <p>
     * {@link DetachedTransaction}'s do implement {@link Closeable}; if th{@code kvstore} is a {@link CloseableKVStore},
     * then it will be {@link CloseableKVStore#close close()}'d if/when the returned {@link DetachedTransaction} is.
     *
     * <p>
     * See {@link #createTransaction(TransactionConfig)} for further details.
     *
     * @param kvstore key/value store, empty or having content compatible with this transaction's {@link Database}
     * @param txConfig transaction configuration
     * @return detached transaction based on {@code kvstore}
     * @throws IllegalArgumentException if {@code kvstore} or {@code txConfig} is null
     */
    public DetachedTransaction createDetachedTransaction(KVStore kvstore, TransactionConfig txConfig) {

        // Validate meta-data
        final Schema schema = this.verifySchemaBundle(kvstore, txConfig);

        // Create detached transaction
        final DetachedTransaction tx = new DetachedTransaction(this, kvstore, schema);

        // Synchronize with all future transaction access
        synchronized (tx) {
            return tx;
        }
    }

    /**
     * Initialize (if necessary) and validate the given {@link KVStore} for use with this database.
     *
     * @param kvstore key/value store
     * @param txConfig transaction configuration
     * @throws IllegalArgumentException if {@code kvstore} or {@code txConfig} is null
     */
    private Schema verifySchemaBundle(KVStore kvstore, TransactionConfig txConfig) {

        // Sanity check
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        Preconditions.checkArgument(txConfig != null, "null txConfig");

        // Get some key prefixes we need
        final byte[] formatKey = Layout.getFormatVersionKey();
        final byte[] metaDataPrefix = Layout.getMetaDataKeyPrefix();
        final byte[] userPrefix = Layout.getUserMetaDataKeyPrefix();
        final byte[] schemaPrefix = Layout.getSchemaTablePrefix();
        final byte[] storageIdPrefix = Layout.getStorageIdTablePrefix();
        final byte[] endOfStorageIdTable = ByteUtil.getKeyAfterPrefix(storageIdPrefix);
        final byte[] schemaIndexPrefix = Layout.getSchemaIndexKeyPrefix();

        // Extract config info
        final SchemaModel schemaModel = txConfig.getSchemaModel();
        assert schemaModel.isLockedDown(true);
        final boolean emptySchema = schemaModel.isEmpty();
        final SchemaId schemaId = schemaModel.getSchemaId();

        // Debug
        if (this.log.isTraceEnabled())
            this.log.trace("creating transaction using schema \"{}\"", schemaId);

        // We will pretend user meta-data is invisible
        final Predicate<byte[]> userMetaData = key -> ByteUtil.isPrefixOf(userPrefix, key);

        // Get iterator over meta-data key/value pairs
        final int formatVersion;
        final boolean uninitialized;
        try (CloseableIterator<KVPair> metaDataIterator = kvstore.getRange(KeyRange.forPrefix(metaDataPrefix))) {

            // Get format version; it should be first; if not found, database is uninitialized (and should be empty)
            byte[] formatVersionBytes = null;
            if (metaDataIterator.hasNext()) {
                final KVPair pair = metaDataIterator.next();
                final byte[] key = pair.getKey();

                assert ByteUtil.isPrefixOf(metaDataPrefix, key);
                if (Arrays.equals(key, formatKey))
                    formatVersionBytes = pair.getValue();
                else if (!userMetaData.test(key)) {
                    throw new InconsistentDatabaseException(String.format(
                      "database is uninitialized but contains unrecognized garbage (key %s)", ByteUtil.toString(key)));
                }
            }

            // If database is uninitialized, initialize it, otherwise validate format version
            uninitialized = formatVersionBytes == null;
            if (uninitialized) {

                // Sanity checks
                final KVPair first = kvstore.getAtLeast(new byte[0], null);
                final KVPair last = kvstore.getAtMost(new byte[] { (byte)0xff }, null);
                if (first != null && !userMetaData.test(first.getKey())) {
                    throw new InconsistentDatabaseException(String.format(
                      "database is uninitialized but contains unrecognized garbage (key %s)", ByteUtil.toString(first.getKey())));
                }
                if (last != null && !userMetaData.test(last.getKey())) {
                    throw new InconsistentDatabaseException(String.format(
                      "database is uninitialized but contains unrecognized garbage (key %s)", ByteUtil.toString(last.getKey())));
                }
                if ((first != null) != (last != null) || (first != null && ByteUtil.compare(first.getKey(), last.getKey()) > 0))
                    throw new InconsistentDatabaseException("inconsistent results from getAtLeast() and getAtMost()");
                try (CloseableIterator<KVPair> testIterator = kvstore.getRange(new byte[0], new byte[] { (byte)0xff })) {
                    if (testIterator.hasNext() ?
                      first == null || !Arrays.equals(testIterator.next().getKey(), first.getKey()) : first != null)
                        throw new InconsistentDatabaseException("inconsistent results from getAtLeast() and getRange()");
                }
                try (CloseableIterator<KVPair> testIterator = kvstore.getRange(new byte[0], new byte[] { (byte)0xff }, true)) {
                    if (testIterator.hasNext() ?
                      last == null || !Arrays.equals(testIterator.next().getKey(), last.getKey()) : last != null)
                        throw new InconsistentDatabaseException("inconsistent results from getAtMost() and getRange()");
                }
                if (!emptySchema)
                    this.checkAddNewSchema(schemaId, txConfig);

                // Initialize database
                formatVersion = Layout.CURRENT_FORMAT_VERSION;
                this.log.debug("detected an uninitialized database; initializing with format version {}", formatVersion);
                final byte[] encodedFormatVersion = UnsignedIntEncoder.encode(formatVersion);
                kvstore.put(formatKey, encodedFormatVersion);

                // Sanity check again
                formatVersionBytes = kvstore.get(formatKey);
                if (formatVersionBytes == null || ByteUtil.compare(formatVersionBytes, encodedFormatVersion) != 0)
                    throw new InconsistentDatabaseException("database failed basic read/write test");
                final KVPair lower = kvstore.getAtLeast(new byte[0], null);
                if (lower == null || !lower.equals(new KVPair(formatKey, encodedFormatVersion)))
                    throw new InconsistentDatabaseException("database failed basic read/write test");
                final KVPair upper = kvstore.getAtMost(userPrefix, null);
                if (upper == null || !upper.equals(new KVPair(formatKey, encodedFormatVersion)))
                    throw new InconsistentDatabaseException("database failed basic read/write test");
            } else {

                // Read format version
                try {
                    formatVersion = UnsignedIntEncoder.decode(formatVersionBytes);
                } catch (IllegalArgumentException e) {
                    throw new InconsistentDatabaseException(String.format(
                      "database contains invalid encoded format version %s under key %s",
                      ByteUtil.toString(formatVersionBytes), ByteUtil.toString(formatKey)));
                }

                // Validate format version
                switch (formatVersion) {
                case Layout.FORMAT_VERSION_1:
                    break;
                default:
                    throw new InconsistentDatabaseException(String.format(
                      "database contains unrecognized version %d under key %s", formatVersion, ByteUtil.toString(formatKey)));
                }
            }

            // There should not be any other meta data prior to the schema table
            if (metaDataIterator.hasNext()) {
                final KVPair pair = metaDataIterator.next();
                final byte[] key = pair.getKey();
                if (ByteUtil.compare(key, schemaPrefix) < 0) {
                    throw new InconsistentDatabaseException(String.format(
                      "database contains unrecognized garbage at key %s", ByteUtil.toString(key)));
                }
            }
        }

        // Read the schema and storage ID tables
        SchemaBundle.Encoded encodedBundle = SchemaBundle.Encoded.readFrom(kvstore);

        // There should not be any meta data between the storage ID table and the object version index
        try (CloseableIterator<KVPair> i = kvstore.getRange(endOfStorageIdTable, schemaIndexPrefix)) {
            if (i.hasNext()) {
                throw new InconsistentDatabaseException(String.format(
                  "database contains unrecognized garbage at key %s", ByteUtil.toString(i.next().getKey())));
            }
        }

        // Grab the EncodingRegistry and also decode bundle data
        final EncodingRegistry txEncodingRegistry;
        SchemaBundle schemaBundle;
        synchronized (this) {

            // Snapshot EncodingRegistry
            txEncodingRegistry = this.encodingRegistry;

            // Optimization: if everything is the same as last time, re-use the previous schema bundle
            if (this.schemaCache != null && this.schemaCache.matches(txConfig, txEncodingRegistry, encodedBundle))
                return this.schemaCache.getSchema();

            // Decode schema and storage ID tables
            schemaBundle = new SchemaBundle(encodedBundle, txEncodingRegistry);
        }

        // Garbage collect unused schemas, if so configured
        boolean modifiedBundle = false;
        if (txConfig.isGarbageCollectSchemas()) {
            final NavigableSet<Integer> listedIndexes = schemaBundle.getSchemasBySchemaIndex().navigableKeySet();
            final NavigableSet<Integer> activeIndexes = Layout.getSchemaIndex(kvstore).asMap().navigableKeySet();
            final NavigableSet<Integer> unusedIndexes = NavigableSets.difference(listedIndexes, activeIndexes);
            for (int oldSchemaIndex : unusedIndexes) {

                // Log it
                final SchemaId oldSchemaId = schemaBundle.getSchema(oldSchemaIndex).getSchemaId();
                this.log.debug("removing old schema \"{}\" from database at schema index {}", oldSchemaId, oldSchemaIndex);

                // Remove old schema
                encodedBundle = schemaBundle.withSchemaRemoved(oldSchemaId);
                schemaBundle = new SchemaBundle(encodedBundle, txEncodingRegistry);
                modifiedBundle = true;
            }
        }

        // If the schema we're using is not registered, we need to add it (unless empty)
        Schema schema;
        if (emptySchema) {
            schema = new Schema(schemaBundle);
            this.log.debug("using empty schema");
        } else if ((schema = schemaBundle.getSchemasBySchemaId().get(schemaId)) == null) {

            // Check whether we can add a new schema
            String schemaList = schemaBundle.getSchemasBySchemaId().keySet().stream()
              .map(id -> String.format("\"%s\"", id))
              .collect(Collectors.joining(", "));
            if (schemaList.isEmpty())
                schemaList = "none";
            this.log.debug("schema \"{}\" not found in database (recorded schemas: {})", schemaId, schemaList);
            this.checkAddNewSchema(schemaId, txConfig);

            // Build new bundle containing new schema
            encodedBundle = schemaBundle.withSchemaAdded(0, schemaModel);
            schemaBundle = new SchemaBundle(encodedBundle, txEncodingRegistry);
            modifiedBundle = true;
            schema = schemaBundle.getSchema(schemaId);
            assert schema != null;

            // Log it
            final int schemaIndex = schemaBundle.getSchema(schemaId).getSchemaIndex();
            this.log.debug("adding new schema \"{}\" to database at schema index {}", schemaId, schemaIndex);
        }

        // Do we need to write back schema updates?
        if (modifiedBundle)
            encodedBundle.writeTo(kvstore);

        // Save for possible reuse next time
        synchronized (this) {
            this.schemaCache = new SchemaCache(schema, txConfig, txEncodingRegistry, encodedBundle);
        }

        // Done
        return schema;
    }

    private void checkAddNewSchema(SchemaId schemaId, TransactionConfig txConfig) {
        if (!txConfig.isAllowNewSchema()) {
            throw new SchemaMismatchException(schemaId, String.format(
              "schema \"%s\" was not found in the database and recording new schemas is disabled", schemaId));
        }
    }

// SchemaCache

    // Used to optimize transaction startup when the schema information in the database has not changed
    private static class SchemaCache {

        private final Schema schema;
        private final TransactionConfig txConfig;
        private final EncodingRegistry encodingRegistry;
        private final SchemaBundle.Encoded encoded;

        SchemaCache(Schema schema, TransactionConfig txConfig, EncodingRegistry encodingRegistry,
          SchemaBundle.Encoded encoded) {
            this.schema = schema;
            this.txConfig = txConfig;
            this.encodingRegistry = encodingRegistry;
            this.encoded = encoded;
        }

        public Schema getSchema() {
            return this.schema;
        }

        public boolean matches(TransactionConfig txConfig, EncodingRegistry encodingRegistry, SchemaBundle.Encoded encoded) {
            if (!txConfig.getSchemaModel().equals(this.txConfig.getSchemaModel()))
                return false;
            if (txConfig.isGarbageCollectSchemas() && !this.txConfig.isGarbageCollectSchemas())
                return false;
            if (!encodingRegistry.equals(this.encodingRegistry))
                return false;
            if (!this.encoded.equals(encoded))
                return false;
            return true;
        }
    }
}
