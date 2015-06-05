
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;

import org.jsimpledb.core.Database;
import org.jsimpledb.core.Schema;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for programmatic {@link JSimpleDB} database access.
 *
 * <p>
 * Instances operate in one of three modes; see {@link SessionMode}.
 * </p>
 *
 * <p>
 * This class is not thread safe.
 * </p>
 *
 * @see SessionMode
 */
public class Session {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final KVDatabase kvdb;
    private final JSimpleDB jdb;
    private final Database db;
    private SessionMode mode;

    private SchemaModel schemaModel;
    private ValidationMode validationMode;
    private NameIndex nameIndex;
    private String databaseDescription;
    private int schemaVersion;
    private boolean allowNewSchema;
    private boolean readOnly;

    private KVTransaction kvt;
    private Transaction tx;

// Constructors

    /**
     * Constructor for {@link SessionMode#KEY_VALUE}.
     *
     * @param kvdb key/value database
     * @throws IllegalArgumentException if {@code kvdb} is null
     */
    public Session(KVDatabase kvdb) {
        this(null, null, kvdb);
    }

    /**
     * Constructor for {@link SessionMode#CORE_API}.
     *
     * @param db core database
     * @throws IllegalArgumentException if {@code db} is null
     */
    public Session(Database db) {
        this(null, db, db != null ? db.getKVDatabase() : null);
    }

    /**
     * Constructor for {@link SessionMode#JSIMPLEDB}.
     *
     * @param jdb database
     * @throws IllegalArgumentException if {@code jdb} is null
     */
    public Session(JSimpleDB jdb) {
        this(jdb, jdb != null ? jdb.getDatabase() : null, jdb != null ? jdb.getDatabase().getKVDatabase() : null);
    }

    private Session(JSimpleDB jdb, Database db, KVDatabase kvdb) {
        Preconditions.checkArgument(kvdb != null, "null kvdb");
        if (jdb != null) {
            Preconditions.checkArgument(db != null, "null db");
            this.mode = SessionMode.JSIMPLEDB;
        } else if (db != null)
            this.mode = SessionMode.CORE_API;
        else
            this.mode = SessionMode.KEY_VALUE;
        this.jdb = jdb;
        this.db = db;
        this.kvdb = kvdb;
    }

// Accessors

    /**
     * Get this instance's {@link SessionMode}.
     *
     * @return this instance's {@link SessionMode}.
     */
    public SessionMode getMode() {
        return this.mode;
    }

    /**
     * Get the associated {@link KVDatabase}.
     *
     * @return the associated {@link KVDatabase}
     */
    public KVDatabase getKVDatabase() {
        return this.kvdb;
    }

    /**
     * Get the associated {@link Database}, if any.
     *
     * @return the associated {@link Database} or null if this instance is not in {@link SessionMode#JSIMPLEDB}
     *  or {@link SessionMode#CORE_API}
     */
    public Database getDatabase() {
        return this.db;
    }

    /**
     * Get the associated {@link JSimpleDB}, if any.
     *
     * @return the associated {@link JSimpleDB} or null if this instance is not in {@link SessionMode#JSIMPLEDB}
     */
    public JSimpleDB getJSimpleDB() {
        return this.jdb;
    }

    /**
     * Get the open {@link KVTransaction} currently associated with this instance.
     *
     * @return the open {@link KVTransaction} in which to do work
     * @throws IllegalStateException if {@link #perform perform()} is not currently being invoked
     */
    public KVTransaction getKVTransaction() {
        Preconditions.checkState(this.kvt != null, "no transaction is currently associated with this session");
        return this.kvt;
    }

    /**
     * Get the open {@link Transaction} currently associated with this instance.
     *
     * @return the open {@link Transaction} in which to do work
     * @throws IllegalStateException if {@link #perform perform()} is not currently being invoked
     * @throws IllegalStateException if this instance is not in mode {@link SessionMode#CORE_API} or {@link SessionMode#JSIMPLEDB}
     */
    public Transaction getTransaction() {
        Preconditions.checkState(this.mode.hasCoreAPI(), "core API not available in " + this.mode);
        Preconditions.checkState(this.tx != null, "no transaction is currently associated with this session");
        return this.tx;
    }

    /**
     * Get the open {@link JTransaction} currently associated with this instance.
     *
     * <p>
     * This method just invokes {@link JTransaction#getCurrent} and returns the result.
     *
     * @return the open {@link JTransaction} in which to do work
     * @throws IllegalStateException if {@link #perform perform()} is not currently being invoked
     * @throws IllegalStateException if this instance is not in mode {@link SessionMode#JSIMPLEDB}
     */
    public JTransaction getJTransaction() {
        Preconditions.checkState(this.mode.hasJSimpleDB(), "JSimpleDB not available in " + this.mode);
        return JTransaction.getCurrent();
    }

    /**
     * Get the {@link SchemaModel} configured for this instance.
     * If this is left unconfigured, after the first transaction it will be updated with the schema model actually used.
     *
     * <p>
     * In {@link SessionMode#KEY_VALUE}, this always returns null.
     *
     * @return the schema model used by this session if available, otherwise null
     */
    public SchemaModel getSchemaModel() {
        return this.schemaModel;
    }
    public void setSchemaModel(SchemaModel schemaModel) {
        this.schemaModel = schemaModel;
        this.nameIndex = this.schemaModel != null ? new NameIndex(this.schemaModel) : null;
    }

    /**
     * Get the {@link NameIndex} for this instance's {@link SchemaModel}.
     *
     * @return name index for the schema model assumed by this session
     */
    public NameIndex getNameIndex() {
        return this.nameIndex != null ? this.nameIndex : new NameIndex(new SchemaModel());
    }

    /**
     * Get a description of the database.
     *
     * @return a short description of the underlying database
     */
    public String getDatabaseDescription() {
        return this.databaseDescription;
    }
    public void setDatabaseDescription(String databaseDescription) {
        this.databaseDescription = databaseDescription;
    }

    /**
     * Get the schema version associated with this instance.
     * If this is left unconfigured, the highest numbered schema version will be
     * used and after the first transaction this property will be updated accordingly.
     *
     * <p>
     * In {@link SessionMode#KEY_VALUE}, this always returns zero.
     *
     * @return the schema version used by this session if known, otherwise zero
     */
    public int getSchemaVersion() {
        return this.schemaVersion;
    }
    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    /**
     * Get the {@link ValidationMode} associated with this instance.
     * If this is left unconfigured, {@link ValidationMode#AUTOMATIC} is used for new transactions.
     *
     * <p>
     * This property is only relevant in {@link SessionMode#JSIMPLEDB}.
     *
     * @return the validation mode used by this session
     */
    public ValidationMode getValidationMode() {
        return this.validationMode;
    }
    public void setValidationMode(ValidationMode validationMode) {
        this.validationMode = validationMode;
    }

    /**
     * Get whether the recording of new schema versions should be allowed.
     * Default value is false.
     *
     * <p>
     * In {@link SessionMode#KEY_VALUE}, this setting is ignored.
     *
     * @return whether this session allows recording a new schema version
     */
    public boolean isAllowNewSchema() {
        return this.allowNewSchema;
    }
    public void setAllowNewSchema(boolean allowNewSchema) {
        this.allowNewSchema = allowNewSchema;
    }

    /**
     * Get whether new transactions should be marked read-only.
     * Default value is false.
     *
     * @return whether this session creates read-only transactions
     */
    public boolean isReadOnly() {
        return this.readOnly;
    }
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

// Errors

    /**
     * Handle an exception thrown during an invocation of {@link #perform perform()}.
     *
     * <p>
     * The implementation in {@code Session} logs an error message. Subclasses are encouraged to
     * handle errors more gracefully within the context of the associated application.
     * </p>
     *
     * @param e exception thrown during {@link #perform perform()}
     */
    protected void reportException(Exception e) {
        this.log.error("exception within session", e);
    }

// Transactions

    /**
     * Perform the given action within a new transaction associated with this instance.
     *
     * <p>
     * If {@code action} throws an {@link Exception}, it will be caught and handled by {@link #reportException reportException()}
     * and then false returned.
     *
     * @param action action to perform
     * @return true if {@code action} completed successfully, false if the transaction could not be created
     *  or {@code action} threw an exception
     * @throws IllegalArgumentException if {@code action} is null
     * @throws IllegalStateException if there is already an open transaction associated with this instance
     */
    public boolean perform(Action action) {

        // Sanity check
        Preconditions.checkArgument(action != null, "null action");
        Preconditions.checkState(this.tx == null || this.kvt != null, "a transaction is already open in this session");

        // Perform action within new transaction
        try {
            if (!this.openTransaction())
                return false;
            boolean success = false;
            try {
                action.run(this);
                success = true;
            } finally {
                success &= this.closeTransaction(success);
            }
            return success;
        } catch (Exception e) {
            this.reportException(e);
            return false;
        } finally {
            this.tx = null;
        }
    }

    private boolean openTransaction() {
        try {

            // Sanity check
            Preconditions.checkState(this.tx == null || this.kvt != null, "a transaction is already open in this session");

            // Open transaction at the appropriate level
            switch (this.mode) {
            case KEY_VALUE:
                this.kvt = this.kvdb.createTransaction();
                break;
            case CORE_API:
                this.tx = this.db.createTransaction(this.schemaModel, this.schemaVersion, this.allowNewSchema);
                this.kvt = this.tx.getKVTransaction();
                break;
            case JSIMPLEDB:
                Preconditions.checkState(!Session.isCurrentJTransaction(),
                  "a JSimpleDB transaction is already open in the current thread");
                final JTransaction jtx = this.jdb.createTransaction(this.allowNewSchema,
                  validationMode != null ? validationMode : ValidationMode.AUTOMATIC);
                JTransaction.setCurrent(jtx);
                this.tx = jtx.getTransaction();
                this.kvt = this.tx.getKVTransaction();
                break;
            default:
                assert false;
                break;
            }

            // Update schema model and version (if appropriate)
            if (this.tx != null) {
                final Schema schema = this.tx.getSchema();
                this.setSchemaModel(schema.getSchemaModel());
                this.setSchemaVersion(schema.getVersionNumber());
                this.tx.setReadOnly(this.readOnly);
            }

            // Done
            return true;
        } catch (Exception e) {
            this.tx = null;
            this.kvt = null;
            this.reportException(e);
            return false;
        }
    }

    @SuppressWarnings("fallthrough")
    private boolean closeTransaction(boolean commit) {
        try {
            Preconditions.checkState(this.tx != null || this.kvt != null, "no transaction");
            switch (this.mode) {
            case JSIMPLEDB:
                if (commit)
                    JTransaction.getCurrent().commit();
                else
                    JTransaction.getCurrent().rollback();
                break;
            case CORE_API:
                if (commit)
                    this.tx.commit();
                else
                    this.tx.rollback();
                break;
            case KEY_VALUE:
                if (commit)
                    this.kvt.commit();
                else
                    this.kvt.rollback();
                break;
            default:
                assert false;
                break;
            }
            return true;
        } catch (Exception e) {
            this.reportException(e);
            return false;
        } finally {
            switch (this.mode) {
            case JSIMPLEDB:
                JTransaction.setCurrent(null);
                // FALLTHROUGH
            case CORE_API:
                this.tx = null;
                // FALLTHROUGH
            case KEY_VALUE:
                this.kvt = null;
                // FALLTHROUGH
            default:
                break;
            }
        }
    }

    private static boolean isCurrentJTransaction() {
        try {
            JTransaction.getCurrent();
            return true;
        } catch (IllegalStateException e) {
            return false;
        }
    }

// Action

    /**
     * Callback interface used by {@link Session#perform Session.perform()}.
     */
    public interface Action {

        /**
         * Perform some action using the given {@link Session} while a transaction is open.
         *
         * @param session session with open transaction
         * @throws Exception if an error occurs
         */
        void run(Session session) throws Exception;
    }
}

