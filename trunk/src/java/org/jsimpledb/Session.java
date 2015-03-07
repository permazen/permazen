
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import org.jsimpledb.core.Database;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for programmatic {@link JSimpleDB} database access.
 *
 * <p>
 * Instances operate in one of two modes, either <b>JSimpleDB</b> mode or <b>core API</b> mode.
 * In the latter mode, no Java model classes are required and only the core API may be used.
 * Of course the core API can also be used in JSimpleDB mode.
 * </p>
 *
 * <p>
 * This class is not thread safe.
 * </p>
 */
public class Session {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final JSimpleDB jdb;
    private final Database db;

    private Transaction tx;
    private SchemaModel schemaModel;
    private ValidationMode validationMode;
    private NameIndex nameIndex;
    private String databaseDescription;
    private int schemaVersion;
    private boolean allowNewSchema;
    private boolean readOnly;

// Constructors

    /**
     * Constructor for core API level access.
     *
     * @param db core database
     * @throws IllegalArgumentException if {@code db} is null
     */
    public Session(Database db) {
        this(null, db);
    }

    /**
     * Constructor for {@link JSimpleDB} level access.
     *
     * @param jdb database
     * @throws IllegalArgumentException if {@code jdb} is null
     */
    public Session(JSimpleDB jdb) {
        this(jdb, jdb.getDatabase());
    }

    private Session(JSimpleDB jdb, Database db) {
        if (db == null)
            throw new IllegalArgumentException("null db");
        this.jdb = jdb;
        this.db = db;
    }

// Accessors

    /**
     * Get the associated {@link JSimpleDB}, if any.
     *
     * @return the associated {@link JSimpleDB} or null if this instance is in core API mode.
     */
    public JSimpleDB getJSimpleDB() {
        return this.jdb;
    }

    /**
     * Determine if this instance has an associated {@link JSimpleDB}.
     * If so it is in JSimpleDB mode, otherwise it's in core API mode.
     *
     * @return true if this session is in JSimpleDB mode, false if in core API mode
     */
    public boolean hasJSimpleDB() {
        return this.jdb != null;
    }

    /**
     * Get the associated {@link Database}.
     *
     * @return the associated {@link Database}
     */
    public Database getDatabase() {
        return this.db;
    }

    /**
     * Get the {@link Transaction} currently associated with this instance.
     * This will be null unless {@link #perform perform()} is currently being invoked.
     *
     * @return the associated {@link Database}
     */
    public Transaction getTransaction() {
        if (this.tx == null)
            throw new IllegalStateException("no transaction associated with session");
        return this.tx;
    }

    /**
     * Get the {@link SchemaModel} configured for this instance.
     * If this is left unconfigured, after the first transaction it will be updated with the schema model actually used.
     *
     * @return the schema model used by this session
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
     * @return the schema version used by this session
     */
    public int getSchemaVersion() {
        return this.schemaVersion;
    }
    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    /**
     * Get the {@link ValidationMode} associated with this instance.
     * This property is only relevant in {@link JSimpleDB} mode.
     * If this is left unconfigured, {@link ValidationMode#AUTOMATIC} is used for new transactions.
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
     * Perform the given action. This is a convenience method, equivalent to: {@code perform(null, action)}
     *
     * @param action action to perform
     * @return true if {@code action} completed successfully, false if the transaction could not be created
     *  or {@code action} threw an exception
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean perform(Action action) {
        return this.perform(null, action);
    }

    /**
     * Perform the given action within the given existing transaction, if any, otherwise within a new transaction.
     * If {@code tx} is not null, it will used and left open when this method returns. Otherwise, if there is already
     * an open transaction associated with this instance, or else the current thread, it will be used and left open when
     * this method returns; otherwise, a new transaction is created for the duration of {@code action} and then committed.
     *
     * <p>
     * If {@code tx} is not null and there is already an open transaction associated with this instance and they
     * are not the same transaction, an {@link IllegalStateException} is thrown.
     * </p>
     *
     * @param tx transaction in which to perform the action, or null to create a new one (if necessary)
     * @param action action to perform
     * @return true if {@code action} completed successfully, false if the transaction could not be created
     *  or {@code action} threw an exception
     * @throws IllegalStateException if {@code tx} conflict with the already an open transaction associated with this instance
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean perform(Transaction tx, final Action action) {

        // Sanity check
        if (action == null)
            throw new IllegalArgumentException("null action");
        if (tx != null && this.tx != null && tx != this.tx)
            throw new IllegalStateException("a transaction is already open in this session");

        // Perform action within (possibly new) transaction
        final Transaction previousTransaction = this.tx;
        try {
            final boolean newTransaction;
            if (tx != null || this.tx != null)
                newTransaction = false;
            else if (this.jdb == null)
                newTransaction = true;
            else {
                final JTransaction jtx = this.getCurrentJTransaction();
                if (jtx == null)
                    newTransaction = true;
                else {
                    tx = jtx.getTransaction();
                    newTransaction = false;
                }
            }
            if (newTransaction) {
                if (!this.openTransaction())
                    return false;
            } else if (this.tx == null)
                this.tx = tx;
            boolean success = false;
            try {
                action.run(this);
                success = true;
            } finally {
                if (newTransaction && this.tx != null) {
                    if (success)
                        success = this.commitTransaction();
                    else
                        this.rollbackTransaction();
                }
            }
            return success;
        } catch (Exception e) {
            this.reportException(e);
            return false;
        } finally {
            this.tx = previousTransaction;
        }
    }

    private boolean openTransaction() {
        try {
            if (this.tx != null)
                throw new IllegalStateException("a transaction is already open in this session");
            if (this.jdb != null) {
                if (this.getCurrentJTransaction() != null)
                    throw new IllegalStateException("a JSimpleDB transaction is already open in the current thread");
                final JTransaction jtx = this.jdb.createTransaction(this.allowNewSchema,
                  validationMode != null ? validationMode : ValidationMode.AUTOMATIC);
                JTransaction.setCurrent(jtx);
                this.tx = jtx.getTransaction();
            } else
                this.tx = this.db.createTransaction(this.schemaModel, this.schemaVersion, this.allowNewSchema);
            final SchemaVersion version = this.tx.getSchemaVersion();
            this.setSchemaModel(version.getSchemaModel());
            this.setSchemaVersion(version.getVersionNumber());
            this.tx.setReadOnly(this.readOnly);
            return true;
        } catch (Exception e) {
            this.tx = null;
            this.reportException(e);
            return false;
        }
    }

    private boolean commitTransaction() {
        try {
            if (this.tx == null)
                throw new IllegalStateException("no transaction");
            if (this.jdb != null)
                JTransaction.getCurrent().commit();
            else
                this.tx.commit();
            return true;
        } catch (Exception e) {
            this.reportException(e);
            return false;
        } finally {
            this.tx = null;
            if (this.jdb != null)
                JTransaction.setCurrent(null);
        }
    }

    private boolean rollbackTransaction() {
        try {
            if (this.tx == null)
                throw new IllegalStateException("no transaction");
            if (this.jdb != null)
                JTransaction.getCurrent().rollback();
            else
                this.tx.rollback();
            return true;
        } catch (Exception e) {
            this.reportException(e);
            return false;
        } finally {
            this.tx = null;
            if (this.jdb != null)
                JTransaction.setCurrent(null);
        }
    }

    private JTransaction getCurrentJTransaction() {
        try {
            return JTransaction.getCurrent();
        } catch (IllegalStateException e) {
            return null;
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

