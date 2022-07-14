
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KVTransactionException;
import io.permazen.schema.SchemaModel;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.Diffs;
import io.permazen.util.UnsignedIntEncoder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides an object database abstraction on top of a key/value database.
 *
 * <p>
 * Includes support for:
 * <ul>
 *  <li>Objects and fields defined by a {@link SchemaModel}, with positive schema verification</li>
 *  <li>Simple values fields containing any atomic type, reference or custom {@link FieldType}</li>
 *  <li>Complex fields of type {@link java.util.List}, {@link java.util.NavigableSet}, and {@link java.util.NavigableMap}</li>
 *  <li>Invertable reference fields with strong referential integrity and configurable delete cascading</li>
 *  <li>Configurable indexing of any simple field or complex sub-field</li>
 *  <li>Composite indexes on multiple simple fields</li>
 *  <li>Notification of object creation and deletion</li>
 *  <li>Notification of object field changes, as seen through an arbitrary path of references</li>
 *  <li>Automatic schema tracking and object versioning with schema change notification support</li>
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
 *  <li>Object references are represented by {@link ObjId}s instead of Java objects, and there is no notion of object sub-type.
 *      However, reference fields may be configured with a restricted set of referrable types.</li>
 *  <li>All object types and fields are referenced by explicit storage ID.</li>
 *  <li>Enum values are represented by {@link EnumValue} objects.</li>
 *  <li>There is no automatic validation support.</li>
 * </ul>
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
    private final FieldTypeRegistry fieldTypeRegistry = new FieldTypeRegistry();
    private final KVDatabase kvdb;

    private volatile Schemas lastSchemas;

    /**
     * Constructor.
     *
     * @param kvdb the underlying key/value store in which to store information
     * @throws IllegalArgumentException if {@code kvdb} is null
     */
    public Database(KVDatabase kvdb) {
        Preconditions.checkArgument(kvdb != null, "null kvdb");
        this.kvdb = kvdb;
    }

    /**
     * Get the {@link FieldTypeRegistry} associated with this instance.
     *
     * @return field type registry associated with this instance
     */
    public FieldTypeRegistry getFieldTypeRegistry() {
        return this.fieldTypeRegistry;
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
     * Create a new transaction.
     *
     * <p>
     * Convenience method; equivalent to:
     *  <blockquote><pre>
     *  createTransaction(schemaModel, version, allowNewSchema, null);
     *  </pre></blockquote>
     *
     * @param schemaModel schema to use with the new transaction, or null to use the schema already recorded in the database
     * @param version the schema version number corresponding to {@code schemaModel},
     *  zero to use the highest version already recorded in the database,
     *  or -1 to use an {@linkplain SchemaModel#autogenerateVersion auto-generated} schema version
     * @param allowNewSchema whether creating a new schema version is allowed
     * @return newly created transaction
     * @throws IllegalArgumentException if {@code version} is less than -1, or equal to -1 when {@code schemaModel} is null
     * @throws InvalidSchemaException if {@code schemaModel} is invalid (i.e., does not pass validation checks)
     * @throws SchemaMismatchException if {@code schemaModel} does not match schema version {@code version}
     *  as recorded in the database
     * @throws SchemaMismatchException if schema version {@code version} is not recorded in the database
     *  and {@code allowNewSchema} is false
     * @throws SchemaMismatchException if schema version {@code version} is not recorded in the database,
     *  {@code allowNewSchema} is true, but {@code schemaModel} is incompatible with one or more other schemas
     *  already recorded in the database (i.e., the same storage ID is used inconsistently between schema versions)
     * @throws SchemaMismatchException
     *  if the database is uninitialized and {@code version == 0} or {@code schemaModel} is null
     * @throws InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws InconsistentDatabaseException if an uninitialized database is encountered but the database is not empty
     * @throws IllegalStateException if no underlying {@link KVDatabase} has been configured for this instance
     */
    public Transaction createTransaction(SchemaModel schemaModel, int version, boolean allowNewSchema) {
        return this.createTransaction(schemaModel, version, allowNewSchema, null);
    }

    /**
     * Create a new {@link Transaction} on this database and use the specified schema version to access objects and fields.
     *
     * <p>
     * <b>Schema Versions</b>
     *
     * <p>
     * Within each {@link Database} is stored a record of all schema versions previously used with the database.
     * When creating a new transaction, the caller provides an expected schema version and corresponding {@link SchemaModel}.
     * Both of these are optional: a schema version of zero means "use the highest version recorded in the
     * database", and a null {@link SchemaModel} means "use the {@link SchemaModel} already recorded in the database under
     * {@code version}".
     *
     * <p>
     * When this method is invoked, the following checks are applied:
     * <ul>
     *  <li>If a schema with version number {@code version != 0} is recorded in the database, and {@code schemaModel} is null or
     *      matches it, then this method succeeds, and the {@link Transaction} will use that schema.</li>
     *  <li>If a schema with version number {@code version} (or the highest numbered schema if {@code version == 0})
     *      is recorded in the database, and {@code schemaModel} is not null and does not match it, then this method fails
     *      and throws a {@link SchemaMismatchException}.</li>
     *  <li>If {@code allowNewSchema} is false, and no schema with version number {@code version != 0} has yet been
     *      recorded in the database, then this method fails and throws a {@link SchemaMismatchException}.</li>
     *  <li>If {@code allowNewSchema} is true, and no schema with version number {@code version != 0} has yet been
     *      recorded in the database, then if {@code schemaModel} is null a {@link SchemaMismatchException} is thrown;
     *      otherwise {@code schemaModel} is checked for compabitility with the schemas previously recorded in the database;
     *      if compatible, this method succeeds, {@code schema} is recorded in the database under the new version number
     *      {@code version}, and the {@link Transaction} will use schema version {@code version};
     *      otherwise a {@link SchemaMismatchException} is thrown.</li>
     *  <li>If the database is uninitialized and {@code version == 0} or {@code schemaModel} is null,
     *      a {@link SchemaMismatchException} is thrown.</li>
     * </ul>
     *
     * <p>
     * For two schemas to "match", they must be identical in all respects, except that object, field, and index names may differ.
     * In the core API, objects and fields are identified by storage ID, not name.
     *
     * <p>
     * Schemas must also be compatible with all other schemas previously recorded in the database.
     * Basically this means storage IDs must be used consistently from a structural point of view:
     * <ul>
     *  <li>Once a storage ID is assigned, it cannot be re-assigned to a different type of item (object or field).</li>
     *  <li>Fields must have a consistent type and structural parent (object type or complex field).</li>
     * </ul>
     *
     * <p>
     * However, object types and fields may be added or removed across schema versions, field indexing may change,
     * and reference field {@link DeleteAction}s may change.
     *
     * <p>
     * <b>Object Versions</b>
     *
     * <p>
     * Each object in a {@link Database} contains an internal version number that indicates its current schema version;
     * this in turn dictates what fields that object contains.
     *
     * <p>
     * When an object is accessed during a {@link Transaction}, the object's version is compared to the {@code version} associated
     * with that {@link Transaction}. If the versions are the same, no version change occurs and fields are accessed normally.
     *
     * <p>
     * If the object has a version {@code oldVersion} different from {@code version}, then depending on which {@link Transaction}
     * method is invoked, the object version may be automatically updated to {@code version}. This will cause fields to be added
     * or removed, as follows:
     * <ul>
     *  <li>Fields that are common to both schema versions remain unchanged (necessarily such fields have the same storage ID,
     *      type, and structural parent).</li>
     *  <li>Fields that exist in {@code oldVersion} but not in {@code version} are removed.</li>
     *  <li>Fields that exist in {@code version} but not in {@code oldVersion} are initialized to their default values.</li>
     *  <li>All {@link VersionChangeListener}s registered with the {@link Transaction} are notified.</li>
     * </ul>
     *
     * <p>
     * Note that compatibility between schema versions does not depend on the field name, nor does it depend on whether the field
     * is indexed, or its {@link DeleteAction} (for reference fields). A field's index may be added or removed between schema
     * versions without losing information, however, querying a field's index will only return those objects whose schema
     * version corresponds to a schema in which the field is indexed. Similarly, the {@link DeleteAction} taken when a
     * referenced object is deleted depends on the {@link DeleteAction} configured in the schema version of the object
     * containing the reference.
     *
     * <p>
     * Note that an object's current schema version can go up as well as down, may change non-consecutively, and in fact
     * nothing requires schema version numbers to be consecutive.
     *
     * @param schemaModel schema to use with the new transaction, or null to use the schema already recorded in the database
     * @param version the schema version number corresponding to {@code schemaModel},
     *  zero to use the highest version already recorded in the database,
     *  or -1 to use an {@linkplain SchemaModel#autogenerateVersion auto-generated} schema version
     * @param allowNewSchema whether creating a new schema version is allowed
     * @param kvoptions optional {@link KVDatabase}-specific transaction options; may be null
     * @return newly created transaction
     * @throws IllegalArgumentException if {@code version} is less than -1, or equal to -1 when {@code schemaModel} is null
     * @throws InvalidSchemaException if {@code schemaModel} is invalid (i.e., does not pass validation checks)
     * @throws SchemaMismatchException if {@code schemaModel} does not match schema version {@code version}
     *  as recorded in the database
     * @throws SchemaMismatchException if schema version {@code version} is not recorded in the database
     *  and {@code allowNewSchema} is false
     * @throws SchemaMismatchException if schema version {@code version} is not recorded in the database,
     *  {@code allowNewSchema} is true, but {@code schemaModel} is incompatible with one or more other schemas
     *  already recorded in the database (i.e., the same storage ID is used inconsistently between schema versions)
     * @throws SchemaMismatchException
     *  if the database is uninitialized and {@code version == 0} or {@code schemaModel} is null
     * @throws InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws InconsistentDatabaseException if an uninitialized database is encountered but the database is not empty
     * @throws IllegalStateException if no underlying {@link KVDatabase} has been configured for this instance
     */
    public Transaction createTransaction(SchemaModel schemaModel, int version, boolean allowNewSchema, Map<String, ?> kvoptions) {
        final KVTransaction kvt = this.kvdb.createTransaction(kvoptions);
        boolean success = false;
        try {
            final Transaction tx = this.createTransaction(kvt, schemaModel, version, allowNewSchema);
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
     * Create a new {@link Transaction} on this database using an already-opened {@link KVTransaction} and specified schema.
     * The given {@link KVTransaction} will be committed or rolled-back along with the returned {@link Transaction}.
     *
     * <p>
     * See {@link #createTransaction(SchemaModel, int, boolean)} for details on schema and object versions.
     *
     * @param kvt already opened key/value store transaction
     * @param schemaModel schema to use with the new transaction, or null to use the schema already recorded in the database
     * @param version the schema version number corresponding to {@code schemaModel},
     *  zero to use the highest version already recorded in the database,
     *  or -1 to use an {@linkplain SchemaModel#autogenerateVersion auto-generated} schema version
     * @param allowNewSchema whether creating a new schema version is allowed
     * @return newly created transaction
     * @throws IllegalArgumentException if {@code kvt} is null
     * @throws IllegalArgumentException if {@code version} is less than -1, or equal to -1 when {@code schemaModel} is null
     * @throws InvalidSchemaException if {@code schemaModel} is invalid (i.e., does not pass validation checks)
     * @throws SchemaMismatchException if {@code schemaModel} does not match schema version {@code version}
     *  as recorded in the database
     * @throws SchemaMismatchException if schema version {@code version} is not recorded in the database
     *  and {@code allowNewSchema} is false
     * @throws SchemaMismatchException if schema version {@code version} is not recorded in the database,
     *  {@code allowNewSchema} is true, but {@code schemaModel} is incompatible with one or more other schemas
     *  already recorded in the database (i.e., the same storage ID is used inconsistently between schema versions)
     * @throws SchemaMismatchException
     *  if the database is uninitialized and {@code version == 0} or {@code schemaModel} is null
     * @throws InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws InconsistentDatabaseException if an uninitialized database is encountered but the database is not empty
     * @throws IllegalStateException if no underlying {@link KVDatabase} has been configured for this instance
     */
    public Transaction createTransaction(KVTransaction kvt, SchemaModel schemaModel, int version, boolean allowNewSchema) {

        // Sanity check
        Preconditions.checkArgument(kvt != null, "null kvt");

        // Validate meta-data
        final Schemas schemas = this.verifySchemas(kvt, schemaModel, version, allowNewSchema);
        assert schemas != null;

        // Create transaction
        return version > 0 ? new Transaction(this, kvt, schemas, version) : new Transaction(this, kvt, schemas);
    }

    /**
     * Create a "snapshot" transaction based on the provided key/value store.
     *
     * <p>
     * The key/value store will be initialized if necessary (i.e., {@code kvstore} may be empty), otherwise it will be
     * validated against the schema information associated with this instance.
     *
     * <p>
     * The returned {@link SnapshotTransaction} does not support {@link SnapshotTransaction#commit commit()},
     * {@link SnapshotTransaction#rollback rollback()}, or {@link SnapshotTransaction#addCallback addCallback()},
     * and can be used indefinitely.
     *
     * <p>
     * If {@code kvstore} is a {@link io.permazen.kv.CloseableKVStore}, then it will be
     * {@link io.permazen.kv.CloseableKVStore#close close()}'d if/when the returned {@link SnapshotTransaction} is.
     *
     * @param kvstore key/value store, empty or having content compatible with this transaction's {@link Database}
     * @param schemaModel schema to use with the new transaction, or null to use the schema already recorded in the database
     * @param version the schema version number corresponding to {@code schemaModel},
     *  zero to use the highest version already recorded in the database,
     *  or -1 to use an {@linkplain SchemaModel#autogenerateVersion auto-generated} schema version
     * @param allowNewSchema whether creating a new schema version in {@code kvstore} is allowed
     * @return snapshot transaction based on {@code kvstore}
     * @throws IllegalArgumentException if {@code version} is less than -1, or equal to -1 when {@code schemaModel} is null
     * @throws InvalidSchemaException if {@code schemaModel} is invalid (i.e., does not pass validation checks)
     * @throws SchemaMismatchException if {@code schemaModel} does not match schema version {@code version}
     *  as recorded in the database
     * @throws SchemaMismatchException if schema version {@code version} is not recorded in the database
     *  and {@code allowNewSchema} is false
     * @throws SchemaMismatchException if schema version {@code version} is not recorded in the database,
     *  {@code allowNewSchema} is true, but {@code schemaModel} is incompatible with one or more other schemas
     *  already recorded in the database (i.e., the same storage ID is used inconsistently between schema versions)
     * @throws SchemaMismatchException
     *  if the database is uninitialized and {@code version == 0} or {@code schemaModel} is null
     * @throws SchemaMismatchException if {@code kvstore} contains incompatible or missing schema information
     * @throws InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws InconsistentDatabaseException if an uninitialized database is encountered but the database is not empty
     * @throws IllegalArgumentException if {@code kvstore} is null
     * @see Transaction#createSnapshotTransaction Transaction.createSnapshotTransaction()
     */
    public SnapshotTransaction createSnapshotTransaction(KVStore kvstore,
      SchemaModel schemaModel, int version, boolean allowNewSchema) {

        // Validate meta-data
        final Schemas schemas = this.verifySchemas(kvstore, schemaModel, version, allowNewSchema);
        assert schemas != null;

        // Create snapshot transaction
        return version > 0 ?
          new SnapshotTransaction(this, kvstore, schemas, version) : new SnapshotTransaction(this, kvstore, schemas);
    }

    /**
     * Initialize (if necessary) and validate the given {@link KVStore} for use with this database.
     *
     * @param kvstore key/value store
     * @param schemaModel schema to use with the new transaction, or null
     * @param version schema version number
     * @param allowNewSchema whether creating a new schema version is allowed
     */
    private Schemas verifySchemas(KVStore kvstore, SchemaModel schemaModel, int version, boolean allowNewSchema) {

        // Sanity check
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        Preconditions.checkArgument(version >= -1, "invalid schema version: " + version);
        Preconditions.checkArgument(schemaModel != null || version >= 0, "can't auto-generate version without schema model");
        if (schemaModel != null) {
            schemaModel.validate();
            if (version == -1)
                version = schemaModel.autogenerateVersion();
        }

        // Debug
        if (this.log.isTraceEnabled()) {
            this.log.trace("creating transaction using "
              + (version != 0 ? "schema version " + version : "highest recorded schema version"));
        }

        // Get iterator over meta-data key/value pairs
        final int formatVersion;
        final boolean uninitialized;
        try (CloseableIterator<KVPair> metaDataIterator = kvstore.getRange(Layout.getMetaDataKeyRange())) {

            // Pretend user meta-data is not there
            final Predicate<byte[]> userMetaData = key -> ByteUtil.isPrefixOf(Layout.getUserMetaDataKeyPrefix(), key);

            // Get format version; it should be first; if not found, database is uninitialized (and should be empty)
            byte[] formatVersionBytes = null;
            if (metaDataIterator.hasNext()) {
                final KVPair pair = metaDataIterator.next();
                assert Layout.getMetaDataKeyRange().contains(pair.getKey());
                if (Arrays.equals(pair.getKey(), Layout.getFormatVersionKey()))
                    formatVersionBytes = pair.getValue();
                else if (!userMetaData.test(pair.getKey())) {
                    throw new InconsistentDatabaseException("database is uninitialized but contains unrecognized garbage (key "
                      + ByteUtil.toString(pair.getKey()) + ")");
                }
            }

            // Get database format object; check for an uninitialized database
            uninitialized = formatVersionBytes == null;
            if (uninitialized) {

                // Sanity checks
                final KVPair first = kvstore.getAtLeast(new byte[0], null);
                final KVPair last = kvstore.getAtMost(new byte[] { (byte)0xff }, null);
                if (first != null && !userMetaData.test(first.getKey())) {
                    throw new InconsistentDatabaseException("database is uninitialized but contains unrecognized garbage (key "
                      + ByteUtil.toString(first.getKey()) + ")");
                }
                if (last != null && !userMetaData.test(last.getKey())) {
                    throw new InconsistentDatabaseException("database is uninitialized but contains unrecognized garbage (key "
                      + ByteUtil.toString(last.getKey()) + ")");
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
                this.checkAddNewSchema(schemaModel, version, allowNewSchema);

                // Initialize database
                formatVersion = Layout.CURRENT_FORMAT_VERSION;
                this.log.debug("detected an uninitialized database; initializing now (format version {})", formatVersion);
                final byte[] encodedFormatVersion = UnsignedIntEncoder.encode(formatVersion);
                kvstore.put(Layout.getFormatVersionKey(), encodedFormatVersion);

                // Sanity check again
                formatVersionBytes = kvstore.get(Layout.getFormatVersionKey());
                if (formatVersionBytes == null || ByteUtil.compare(formatVersionBytes, encodedFormatVersion) != 0)
                    throw new InconsistentDatabaseException("database failed basic read/write test");
                final KVPair lower = kvstore.getAtLeast(new byte[0], null);
                if (lower == null || !lower.equals(new KVPair(Layout.getFormatVersionKey(), encodedFormatVersion)))
                    throw new InconsistentDatabaseException("database failed basic read/write test");
                final KVPair upper = kvstore.getAtMost(Layout.getUserMetaDataKeyPrefix(), null);
                if (upper == null || !upper.equals(new KVPair(Layout.getFormatVersionKey(), encodedFormatVersion)))
                    throw new InconsistentDatabaseException("database failed basic read/write test");
            } else {

                // Read format version
                try {
                    formatVersion = UnsignedIntEncoder.decode(formatVersionBytes);
                } catch (IllegalArgumentException e) {
                    throw new InconsistentDatabaseException("database contains invalid encoded format version "
                      + ByteUtil.toString(formatVersionBytes) + " under key " + ByteUtil.toString(Layout.getFormatVersionKey()));
                }

                // Validate format version
                switch (formatVersion) {
                case Layout.FORMAT_VERSION_1:
                case Layout.FORMAT_VERSION_2:
                    break;
                default:
                    throw new InconsistentDatabaseException("database contains unrecognized format version "
                      + formatVersion + " under key " + ByteUtil.toString(Layout.getFormatVersionKey()));
                }
            }

            // There should not be any other meta data prior to recorded schemas
            if (metaDataIterator.hasNext()) {
                final KVPair pair = metaDataIterator.next();
                if (!Layout.getSchemaKeyRange().contains(pair.getKey())) {
                    throw new InconsistentDatabaseException("database contains unrecognized garbage at key "
                      + ByteUtil.toString(pair.getKey()));
                }
            }
        }

        // Check schema
        Schemas schemas = null;
        for (boolean firstAttempt = true; true; firstAttempt = false) {

            // Read recorded database schema versions
            final TreeMap<Integer, byte[]> bytesMap = new TreeMap<>();
            try (CloseableIterator<KVPair> i = kvstore.getRange(Layout.getSchemaKeyRange())) {
                while (i.hasNext()) {
                    final KVPair pair = i.next();
                    assert Layout.getSchemaKeyRange().contains(pair.getKey());

                    // Decode schema version and get XML
                    final int vers = UnsignedIntEncoder.read(new ByteReader(pair.getKey(), Layout.getSchemaKeyPrefix().length));
                    if (vers == 0)
                        throw new InconsistentDatabaseException("database contains an invalid schema version zero");
                    bytesMap.put(vers, pair.getValue());
                }
            }

            // Read and decode database schemas, avoiding rebuild if possible
            schemas = this.lastSchemas;
            if (schemas != null && !schemas.isSameVersions(bytesMap))
                schemas = null;
            if (schemas == null) {
                try {
                    schemas = this.decodeSchemas(bytesMap, formatVersion);
                } catch (IllegalArgumentException e) {
                    if (firstAttempt)
                        throw new InconsistentDatabaseException("database contains invalid schema information", e);
                    else
                        throw new InvalidSchemaException("schema is not valid: " + e.getMessage(), e);
                }
            }

            // If no version specified, assume the highest recorded version
            if (version == 0 && !bytesMap.isEmpty())
                version = bytesMap.lastKey();

            // If transaction schema was not found in the database, add it and retry
            if (!bytesMap.containsKey(version)) {

                // Log it
                if (bytesMap.isEmpty()) {
                    if (!uninitialized)
                        throw new InconsistentDatabaseException("database is initialized but contains no recorded schema versions");
                } else
                    this.log.debug("schema version {} not found in database; known versions are {}", version, bytesMap.keySet());

                // Check whether we can add a new schema version
                this.checkAddNewSchema(schemaModel, version, allowNewSchema);

                // Record new schema in database
                this.log.debug("recording new schema version {} into database", version);
                kvstore.put(Layout.getSchemaKey(version), Layout.encodeSchema(schemaModel, formatVersion));

                // Try again
                schemas = null;
                continue;
            }

            // Compare transaction schema with the schema of the same version found in the database
            if (this.log.isTraceEnabled())
                this.log.trace("found schema version {} in database; known versions are {}", version, bytesMap.keySet());
            final SchemaModel dbSchemaModel = schemas.getVersion(version).getSchemaModel();
            if (schemaModel != null) {
                if (!schemaModel.isCompatibleWith(dbSchemaModel)) {
                    final Diffs diffs = schemaModel.differencesFrom(dbSchemaModel);
                    this.log.error("schema mismatch:\n=== Database schema ===\n{}\n=== Provided schema ===\n{}"
                      + "\n=== Differences ===\n{}", dbSchemaModel, schemaModel, diffs);
                    throw new IllegalArgumentException("the provided schema is not compatible with the schema already recorded"
                      + " in the database under version " + version + ":\n" + diffs);
                } else if (this.log.isTraceEnabled() && !schemaModel.equals(dbSchemaModel)) {
                    final Diffs diffs = schemaModel.differencesFrom(dbSchemaModel);
                    this.log.trace("the provided schema differs from, but is compatible with, the database schema:\n{}", diffs);
                }
            }
            break;
        }

        // Save schema for next time
        this.lastSchemas = schemas;

        // Done
        return schemas;
    }

    /**
     * Validate a {@link SchemaModel} against this instance.
     *
     * <p>
     * This is a convenience method, equivalent to: {@link #validateSchema(FieldTypeRegistry, SchemaModel)
     * Database.validateSchema}{@code (this.getFieldTypeRegistry(), schemaModel)}.
     *
     * @param schemaModel schema to validate
     * @throws InvalidSchemaException if {@code schemaModel} is invalid
     * @throws IllegalArgumentException if {@code schemaModel} is null
     */
    public void validateSchema(SchemaModel schemaModel) {
        Database.validateSchema(this.getFieldTypeRegistry(), schemaModel);
    }

    /**
     * Validate a {@link SchemaModel}.
     *
     * <p>
     * This method only statically checks the schema; it does not validate the schema against any other
     * existing schema versions that may be previously recorded in a database.
     *
     * <p>
     * To validate a schema against a particular database, simply attempt to create a transaction
     * via {@link #createTransaction createTransaction()}. To validate that a collection of schemas
     * are mutually consistent independently from any database, use {@link #validateSchemas validateSchemas()}.
     *
     * @param fieldTypeRegistry registry of simple field types
     * @param schemaModel schema to validate
     * @throws InvalidSchemaException if {@code schemaModel} is invalid
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void validateSchema(FieldTypeRegistry fieldTypeRegistry, SchemaModel schemaModel) {

        // Sanity check
        Preconditions.checkArgument(fieldTypeRegistry != null, "null fieldTypeRegistry");
        Preconditions.checkArgument(schemaModel != null, "null schemaModel");

        // Validate
        schemaModel.validate();
        try {
            new Schema(1, new byte[0], schemaModel, fieldTypeRegistry);
        } catch (IllegalArgumentException e) {
            throw new InvalidSchemaException("invalid schema: " + e.getMessage(), e);
        }
    }

    /**
     * Check whether a collection of {@link SchemaModel}s are individually valid and mutually consistent.
     *
     * <p>
     * This method verifies each schema via {@link #validateSchema(FieldTypeRegistry, SchemaModel) validateSchema()},
     * and also verifies that the schemas are mututally consistent, i.e., that they do not use storage ID's incompatibly.
     *
     * @param fieldTypeRegistry registry of simple field types
     * @param schemaModels schemas to validate (null elements are ignored)
     * @throws InvalidSchemaException if an element in {@code schemaModels} is invalid
     * @throws InvalidSchemaException if the {@code schemaModels} are not mutally consistent
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void validateSchemas(FieldTypeRegistry fieldTypeRegistry, Collection<SchemaModel> schemaModels) {

        // Sanity check
        Preconditions.checkArgument(fieldTypeRegistry != null, "null fieldTypeRegistry");
        Preconditions.checkArgument(schemaModels != null, "null schemaModels");

        // Validate schemas individually and build map
        final TreeMap<Integer, Schema> schemaMap = new TreeMap<>();
        int index = 0;
        for (SchemaModel schemaModel : schemaModels) {
            if (schemaModel == null)
                continue;
            schemaModel.validate();
            final int version = index + 1;
            try {
                schemaMap.put(version, new Schema(version, new byte[0], schemaModel, fieldTypeRegistry));
            } catch (IllegalArgumentException e) {
                throw new InvalidSchemaException("invalid schema at index " + index + ": " + e.getMessage(), e);
            }
            index++;
        }

        // Validate schemas together
        try {
            new Schemas(schemaMap);
        } catch (InvalidSchemaException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidSchemaException("invalid schema combination: " + e.getMessage(), e);
        }
    }

    private void checkAddNewSchema(SchemaModel schemaModel, int version, boolean allowNewSchema) {
        if (version == 0)
            throw new SchemaMismatchException("database is uninitialized and no schema version was provided");
        if (schemaModel == null) {
            throw new SchemaMismatchException("schema version " + version
              + " was not found in database, and no schema model was provided");
        }
        if (!allowNewSchema) {
            throw new SchemaMismatchException("schema version " + version
              + " was not found in database, and recording a new schema version is disabled in this transaction");
        }
    }

    /**
     * Build {@link Schemas} object from a schema version XMLs.
     *
     * @throws InconsistentDatabaseException if any recorded schema version is invalid
     */
    private Schemas decodeSchemas(SortedMap<Integer, byte[]> bytesMap, int formatVersion) {
        final TreeMap<Integer, Schema> versionMap = new TreeMap<>();
        for (Map.Entry<Integer, byte[]> entry : bytesMap.entrySet()) {
            final int version = entry.getKey();
            final byte[] bytes = entry.getValue();
            final SchemaModel schemaModel;
            try {
                schemaModel = Layout.decodeSchema(bytes, formatVersion);
            } catch (InvalidSchemaException e) {
                throw new InconsistentDatabaseException("found invalid schema version " + version + " recorded in database", e);
            }
            if (this.log.isTraceEnabled())
                this.log.trace("read schema version {} from database:\n{}", version, schemaModel);
            versionMap.put(version, new Schema(version, bytes, schemaModel, this.fieldTypeRegistry));
        }
        return new Schemas(versionMap);
    }
}

