
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides an object database abstraction on top of a key/value database.
 *
 * <p>
 * Includes support for:
 * <ul>
 *  <li>Objects and fields defined by a {@link SchemaModel}</li>
 *  <li>Simple values fields containing any atomic type</li>
 *  <li>User-defined custom {@link FieldType}s</li>
 *  <li>Invertable reference fields with strong referential integrity and configurable delete cascading</li>
 *  <li>Complex fields of type {@link java.util.NavigableSet}, {@link java.util.List}, and {@link java.util.NavigableMap}</li>
 *  <li>Configurable indexing of any simple or complex field</li>
 *  <li>Notification of field changes as seen through an arbitrary path of references</li>
 *  <li>Automatic schema tracking and object versioning with schema change notification support</li>
 * </ul>
 * </p>
 *
 * <p>
 * See {@link Transaction} for further details on the above functionality.
 * </p>
 *
 * <p>
 * This class defines an abstraction layer that usually sits below a {@link org.jsimpledb.JSimpleDB} but is completely
 * independent of {@link org.jsimpledb.JSimpleDB} and can be used on its own.
 * Compared to {@link org.jsimpledb.JSimpleDB}, a {@link Database} has these differences:
 * <ul>
 *  <li>A {@link SchemaModel} must be explicitly provided to define the schema in use</li>
 *  <li>Database objects are represented by {@link ObjId}s instead of Java objects, and there is no notion of object sub-type</li>
 *  <li>All object types and fields must be referenced by storage ID using a {@link Transaction} object</li>
 *  <li>There is no automatic validation support</li>
 * </ul>
 * </p>
 *
 * @see Transaction
 * @see org.jsimpledb
 */
public class Database {

    // Special key prefix reserved for JSimpleDB
    private static final byte JSIMPLEDB_KEY_PREFIX = (byte)0x00;

    // Special JSimpleDB keys
    private static final byte[] ENCODING_KEY = new byte[] {
      JSIMPLEDB_KEY_PREFIX, (byte)0x8f, (byte)0xb5, (byte)0x3f, (byte)0xe9, (byte)0xda, (byte)0x1c, (byte)0xc9
    };
    private static final byte[] SCHEMA_KEY_PREFIX = new byte[] {
      JSIMPLEDB_KEY_PREFIX, (byte)0x01
    };

    // Current JSimpleDB encoding version number
    private static final int ENCODING_VERSION = 1;

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final FieldTypeRegistry fieldTypeRegistry = new FieldTypeRegistry();

    private KVDatabase kvdb;
    private Schema lastSchema;

    /**
     * Default Constructor. Caller must separately configure the {@link KVDatabase} to use via {@link #setKVDatabase}.
     */
    public Database() {
    }

    /**
     * Constructor.
     *
     * @param kvdb the underlying key/value store in which to store information
     * @throws IllegalArgumentException if {@code kvdb} is null
     */
    public Database(KVDatabase kvdb) {
        if (kvdb == null)
            throw new IllegalArgumentException("null kvdb");
        this.kvdb = kvdb;
    }

    /**
     * Get the {@link FieldTypeRegistry} associated with this instance.
     */
    public FieldTypeRegistry getFieldTypeRegistry() {
        return this.fieldTypeRegistry;
    }

    /**
     * Get the {@link KVDatabase} underlying this instance.
     */
    public synchronized KVDatabase getKVDatabase() {
        return this.kvdb;
    }

    /**
     * Set the {@link KVDatabase} underlying this instance.
     *
     * <p>
     * Transactions that have already been created are not affected, but new ones will be.
     * </p>
     *
     * @throws IllegalArgumentException if {@code kvdb} is null
     */
    public synchronized void setKVDatabase(KVDatabase kvdb) {
        if (kvdb == null)
            throw new IllegalArgumentException("null kvdb");
        this.kvdb = kvdb;
    }

    /**
     * Create a new {@link Transaction} on this database and use the specified schema version to access objects and fields.
     *
     * <p>
     * <b>Schema Versions</b>
     * </p>
     *
     * <p>
     * Within each {@link Database} is stored a record of all schema versions previously used with the database.
     * When creating a new transaction, the caller provides an expected schema version and corresponding {@link SchemaModel}.
     * Both of these are optional: a schema version of zero means "use the highest version recorded in the
     * database", and a null {@link SchemaModel} measn "use the {@link SchemaModel} already recorded in the database under
     * {@code version}". When this method is invoked, the following checks are applied:
     * <ul>
     *  <li>If a schema with version number {@code version != 0} is recorded in the database, and {@code schemaModel} is null or
     *      matches it, then this method succeeds, and the {@link Transaction} will use that schema.</li>
     *  <li>If a schema with version number {@code version} (or the highest numbered schema if {@code version == 0})
     *      is recorded in the database, and {@code schemaModel} is not null and does not match it, then this method fails
     *      and throws {@link SchemaMismatchException}.</li>
     *  <li>If {@code allowNewSchema} is false, and no schema with version number {@code version != 0} has yet been
     *      recorded in the database, then this method fails and throws {@link SchemaMismatchException}.</li>
     *  <li>If {@code allowNewSchema} is true, and no schema with version number {@code version != 0} has yet been
     *      recorded in the database, then if {@code schemaModel} is null an {@link SchemaMismatchException} is thrown;
     *      otherwise {@code schemaModel} is checked for compabitility with all of the other schemas recorded in the database;
     *      if compatible, this method succeeds, {@code schema} is recorded in the database with version number {@code version},
     *      and the {@link Transaction} will use schema version {@code version}; otherwise an {@link SchemaMismatchException}
     *      is thrown.</li>
     *  <li>If the database is uninitialized and {@code version == 0} or {@code schemaModel} is null,
     *      a {@link SchemaMismatchException} is thrown.</li>
     * </ul>
     * </p>
     *
     * <p>
     * Fields and objects have assigned storage IDs, and these identify the schema object across all schema versions.
     * Once a storage ID is assigned, it cannot be re-assigned to a different schema object. Fields must have a consistent
     * type and parent entity (object type or complex field) in all schema versions in which they appear.
     * </p>
     *
     * <p>
     * <b>Object Versions</b>
     * </p>
     *
     * <p>
     * Each object in a {@link Database} contains an internal version number that indicates its current schema version;
     * this in turn dictates what fields that object contains.
     * </p>
     *
     * <p>
     * When an object is accessed during a {@link Transaction}, the object's version is compared to the {@code version} associated
     * with that {@link Transaction}. If the versions are the same, no version change occurs and fields are accessed normally.
     * </p>
     *
     * <p>
     * If the object has a version {@code oldVersion} different from {@code version}, then depending on which {@link Transaction}
     * method is invoked, the object version may be automatically updated to {@code version}. This will cause fields to be added
     * or removed, as follows:
     * <ul>
     *  <li>Compatible fields that are common to both schema versions remain unchanged; fields are compatible
     *      if they have the same storage ID and type. For complex fields, sub-fields must also be compatible.</li>
     *  <li>Fields that exist in {@code oldVersion} but not in {@code version} are removed.</li>
     *  <li>Fields that exist in {@code version} but not in {@code oldVersion} are initialized to their default values.</li>
     *  <li>All {@link VersionChangeListener}s registered with the {@link Transaction} are notified.</li>
     * </ul>
     * </p>
     *
     * <p>
     * Note that compatibility between schema versions does not depend on the field name, or whether the field is indexed.
     * Fields are identified by storage ID, not name. A field's index may be added or removed between schema versions
     * without losing information. Note, however, that querying a field's index will only return objects whose schema version
     * corresponds to a schema in which the field is indexed.
     * </p>
     *
     * <p>
     * Note that an object's current schema version can go up as well as down, and may change by any amount.
     * Also, nothing requires schema version numbers to be consecutive.
     * </p>
     *
     * @param schemaModel schema to use with the new transaction, or null to use the schema already recorded in the database
     * @param version the schema version number corresponding to {@code schemaModel}, or zero to use the highest recorded version
     * @param allowNewSchema whether creating a new schema version is allowed
     * @throws IllegalArgumentException if {@code version} is less than zero
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
     * @throws InconsistentDatabaseException if inconsistent or invalid schema information is detected in the database
     * @throws InconsistentDatabaseException if an uninitialized database is encountered but the database is not empty
     * @throws IllegalStateException if no underlying {@link KVDatabase} has been configured for this instance
     */
    public Transaction createTransaction(final SchemaModel schemaModel, int version, final boolean allowNewSchema) {

        // Sanity check
        if (version < 0)
            throw new IllegalArgumentException("invalid schema version: " + version);

        // Validate schema
        if (schemaModel != null)
            schemaModel.validate();

        // Open KV transaction
        final KVTransaction kvt;
        synchronized (this) {
            if (this.kvdb == null)
                throw new IllegalStateException("no KVDatabase configured");
            kvt = this.kvdb.createTransaction();
        }
        boolean success = false;
        this.log.debug("creating transaction using "
          + (version != 0 ? "schema version " + version : "highest recorded schema version"));
        try {
            Schema schema = null;
            boolean firstAttempt = true;
            while (true) {

                // Check for an uninitialized database
                boolean uninitialized = false;
                byte[] encodingBytes = kvt.get(ENCODING_KEY);
                if (encodingBytes == null) {

                    // Sanity checks
                    if (kvt.getAtLeast(new byte[0]) != null)
                        throw new InconsistentDatabaseException("database is uninitialized but contains unrecognized garbage");
                    if (kvt.getAtMost(new byte[] { (byte)0xff }) != null)
                        throw new InconsistentDatabaseException("inconsistent results from getAtLeast() and getAtMost()");

                    // Initialize database
                    this.log.info("detected an uninitialized database; initializing");
                    final ByteWriter writer = new ByteWriter();
                    UnsignedIntEncoder.write(writer, ENCODING_VERSION);
                    kvt.put(ENCODING_KEY, writer.getBytes());
                    uninitialized = true;

                    // Sanity check again
                    encodingBytes = kvt.get(ENCODING_KEY);
                    if (encodingBytes == null || ByteUtil.compare(encodingBytes, writer.getBytes()) != 0)
                        throw new InconsistentDatabaseException("database failed basic read/write test");
                }

                // Verify encoding version
                final int encodingVersion;
                try {
                    encodingVersion = UnsignedIntEncoder.read(new ByteReader(encodingBytes));
                } catch (IllegalArgumentException e) {
                    throw new InconsistentDatabaseException("database contains invalid encoding version under key "
                      + ByteUtil.toString(encodingBytes));
                }
                switch (encodingVersion) {
                case ENCODING_VERSION:
                    break;
                default:
                    throw new InconsistentDatabaseException("database contains unrecognized encoding version "
                      + encodingVersion + " under key " + ByteUtil.toString(encodingBytes));
                }

                // Read existing database schema versions
                final TreeMap<Integer, byte[]> bytesMap = new TreeMap<>();
                for (Iterator<KVPair> i = kvt.getRange(SCHEMA_KEY_PREFIX, ByteUtil.getKeyAfterPrefix(SCHEMA_KEY_PREFIX), false);
                  i.hasNext(); ) {
                    final KVPair pair = i.next();
                    final int vers = UnsignedIntEncoder.read(new ByteReader(pair.getKey(), SCHEMA_KEY_PREFIX.length));
                    bytesMap.put(vers, pair.getValue());
                }

                // Read and decode database schemas, avoiding rebuild if possible
                synchronized (this) {
                    if (this.lastSchema != null && this.lastSchema.isSameVersions(bytesMap))
                        schema = this.lastSchema;
                }
                if (schema == null) {
                    try {
                        schema = this.buildSchema(bytesMap);
                    } catch (IllegalArgumentException e) {
                        if (firstAttempt)
                            throw new InconsistentDatabaseException("database contains invalid schema information", e);
                        else
                            throw new InvalidSchemaException("schema is not valid", e);
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
                            throw new InconsistentDatabaseException("database is initialized but contains zero schema versions");
                    } else {
                        this.log.info("schema version " + version
                          + " not found in database; known versions are " + bytesMap.keySet());
                    }

                    // Check whether we can add a new schema version
                    if (!allowNewSchema) {
                        if (version == 0)
                            throw new SchemaMismatchException("uninitialized database and no schema version was provided");
                        else {
                            throw new SchemaMismatchException("schema version " + version
                              + " not found in database and allowNewSchema is false");
                        }
                    }
                    if (schemaModel == null) {
                        if (version == 0)
                            throw new SchemaMismatchException("uninitialized database and no schema model was provided");
                        else {
                            throw new SchemaMismatchException("no schema model was provided but database does not contain"
                              + " a recorded schema version " + version);
                        }
                    }

                    // Record new schema in database
                    this.log.info("recording new schema version " + version + " into database");
                    this.writeSchema(kvt, version, schemaModel);

                    // Try again
                    schema = null;
                    firstAttempt = false;
                    continue;
                }

                // Compare transaction schema with the schema of the same version found in the database
                this.log.debug("found schema version " + version + " in database; known versions are " + bytesMap.keySet());
                final SchemaModel dbSchemaModel = schema.getVersion(version).getSchemaModel();
                if (schemaModel != null && !schemaModel.equals(schema.getVersion(version).getSchemaModel())) {
                    this.log.error("schema mismatch:\n  database schema:\n{}  provided schema:\n{}", dbSchemaModel, schemaModel);
                    throw new IllegalArgumentException("the provided transaction schema does not match the schema with version "
                      + version + " that is already recorded in the database");
                }
                break;
            }

            // Save schema for next time
            synchronized (this) {
                this.lastSchema = schema;
            }

            // Create transaction
            final Transaction tx = new Transaction(this, kvt, schema, version);
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
     * Validate a {@link SchemaModel}.
     *
     * <p>
     * This method only performs "static" checks; it does not access the database and therefore
     * does not validate the schema against existing schema versions previously recorded.
     * It does however use the {@link FieldTypeRegistry} associated with this instance to look up field types.
     * </p>
     *
     * <p>
     * To validate a schema against the database contents as well, simply attempt to create a transaction
     * via {@link #createTransaction createTransaction()}.
     * </p>
     *
     * @param schemaModel schema to validate
     * @throws InvalidSchemaException if {@code schemaModel} is invalid
     * @throws IllegalArgumentException if {@code schemaModel} is null
     */
    public void validateSchema(SchemaModel schemaModel) {

        // Sanity check
        if (schemaModel == null)
            throw new IllegalArgumentException("null schemaModel");

        // Validate
        schemaModel.validate();
        try {
            new SchemaVersion(1, new byte[0], schemaModel, this.fieldTypeRegistry);
        } catch (IllegalArgumentException e) {
            throw new InvalidSchemaException("invalid schema: " + e.getMessage(), e);
        }
    }

    /**
     * Build {@link Schema} object from a schema version XMLs.
     *
     * @throws InconsistentDatabaseException if any recorded schema version is invalid
     */
    private Schema buildSchema(SortedMap<Integer, byte[]> bytesMap) {
        final TreeMap<Integer, SchemaVersion> versionMap = new TreeMap<>();
        for (Map.Entry<Integer, byte[]> entry : bytesMap.entrySet()) {
            final int version = entry.getKey();
            final byte[] bytes = entry.getValue();
            final SchemaModel schemaModel;
            try {
                schemaModel = this.decodeSchema(bytes);
            } catch (InvalidSchemaException e) {
                throw new InconsistentDatabaseException("found invalid schema version " + version + " recorded in database", e);
            }
            if (this.log.isTraceEnabled())
                this.log.trace("read schema version {} from database:\n{}", version, schemaModel);
            versionMap.put(version, new SchemaVersion(version, bytes, schemaModel, this.fieldTypeRegistry));
        }
        return new Schema(versionMap);
    }

    /**
     * Decode and validate schema XML.
     *
     * @throws InvalidSchemaException if schema is invalid
     */
    private SchemaModel decodeSchema(byte[] value) {
        try {
            return SchemaModel.fromXML(new ByteArrayInputStream(value));
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }
    }

    /**
     * Record the given schema into the database.
     */
    private void writeSchema(KVTransaction kvt, int version, SchemaModel schema) {

        // Encode as XML
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            schema.toXML(buf, false);
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }

        // Write XML
        final ByteWriter writer = new ByteWriter();
        writer.write(SCHEMA_KEY_PREFIX);
        UnsignedIntEncoder.write(writer, version);
        kvt.put(writer.getBytes(), buf.toByteArray());
    }
}

