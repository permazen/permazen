
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;

import java.util.Map;

import org.jsimpledb.core.Database;
import org.jsimpledb.core.Schema;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for programmatic {@link JSimpleDB} database access.
 *
 * <p>
 * Instances operate in one of three modes; see {@link SessionMode}.
 *
 * <p>
 * Instances are <b>not</b> thread safe.
 *
 * @see SessionMode
 */
public class Session {

    /**
     * Default value for the {@linkplain #getMaxRetries maximum number of retry attempts}.
     */
    public static final int DEFAULT_MAX_RETRIES = 6;

    /**
     * Default value for the {@linkplain #getInitialRetryDelay initial retry delay} (in milliseconds).
     */
    public static final int DEFAULT_INITIAL_RETRY_DELAY = 100;

    /**
     * Default value for the {@linkplain #getMaximumRetryDelay maximum retry delay} (in milliseconds).
     */
    public static final int DEFAULT_MAXIMUM_RETRY_DELAY = 2500;

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
    private boolean rollbackOnly;

    // Retry settings
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private int initialRetryDelay = DEFAULT_INITIAL_RETRY_DELAY;
    private int maximumRetryDelay = DEFAULT_MAXIMUM_RETRY_DELAY;

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
     * Change this instance's {@link SessionMode}.
     *
     * @param mode new {@link SessionMode}
     * @throws IllegalArgumentException if {@code mode} is null
     * @throws IllegalArgumentException if {@code mode} requires a {@link JSimpleDB} (i.e., {@link SessionMode#JSIMPLEDB}),
     *  or {@link Database} (i.e., {@link SessionMode#CORE_API}) instance, but none was provided at construction
     */
    public void setMode(SessionMode mode) {
        Preconditions.checkArgument(mode != null, "null mode");
        switch (mode) {
        case KEY_VALUE:
            break;
        case CORE_API:
            Preconditions.checkArgument(this.db != null, "session is not configured with a Core API Database instance");
            break;
        case JSIMPLEDB:
            Preconditions.checkArgument(this.jdb != null, "session is not configured with a JSimpleDB instance");
            break;
        default:
            throw new IllegalArgumentException("internal error");
        }
        this.mode = mode;
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
     * @throws IllegalStateException if {@link #performSessionAction performSessionAction()} is not currently being invoked
     */
    public KVTransaction getKVTransaction() {
        Preconditions.checkState(this.kvt != null, "no transaction is currently associated with this session");
        return this.kvt;
    }

    /**
     * Get the open {@link Transaction} currently associated with this instance.
     *
     * @return the open {@link Transaction} in which to do work
     * @throws IllegalStateException if {@link #performSessionAction performSessionAction()} is not currently being invoked
     * @throws IllegalStateException if this instance is not in mode {@link SessionMode#CORE_API} or {@link SessionMode#JSIMPLEDB}
     */
    public Transaction getTransaction() {
        Preconditions.checkState(this.mode.hasCoreAPI(), "core API not available in " + this.mode + " mode");
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
     * @throws IllegalStateException if {@link #performSessionAction performSessionAction()} is not currently being invoked
     * @throws IllegalStateException if this instance is not in mode {@link SessionMode#JSIMPLEDB}
     */
    public JTransaction getJTransaction() {
        Preconditions.checkState(this.mode.hasJSimpleDB(), "JSimpleDB not available in " + this.mode + " mode");
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

    /**
     * Get the maximum number of allowed retries when a {@link RetryableAction} is given to
     * {@link #performSessionAction performSessionAction()}.
     *
     * <p>
     * Default value is {@link #DEFAULT_MAX_RETRIES}.
     *
     * @return maximum number of retry attempts, or zero if retries are disabled
     * @see Session#performSessionAction Session.performSessionAction()
     */
    public int getMaxRetries() {
        return this.maxRetries;
    }
    public void setMaxRetries(int maxRetries) {
        Preconditions.checkArgument(maxRetries >= 0, "maxRetries < 0");
        this.maxRetries = maxRetries;
    }

    /**
     * Get the initial retry delay when a {@link RetryableAction} is given to
     * {@link #performSessionAction performSessionAction()}.
     *
     * <p>
     * Default value is {@link #DEFAULT_INITIAL_RETRY_DELAY}.
     *
     * @return initial retry delay in milliseconds
     * @see Session#performSessionAction Session.performSessionAction()
     */
    public int getInitialRetryDelay() {
        return this.initialRetryDelay;
    }
    public void setInitialRetryDelay(int initialRetryDelay) {
        Preconditions.checkArgument(initialRetryDelay > 0, "initialRetryDelay < 0");
        this.initialRetryDelay = initialRetryDelay;
    }

    /**
     * Configure the maximum retry delay when a {@link RetryableAction} is given to
     * {@link #performSessionAction performSessionAction()}.
     *
     * <p>
     * Default value is {@link #DEFAULT_MAXIMUM_RETRY_DELAY}.
     *
     * @return maximum retry delay in milliseconds
     * @see Session#performSessionAction Session.performSessionAction()
     */
    public int getMaximumRetryDelay() {
        return this.maximumRetryDelay;
    }
    public void setMaximumRetryDelay(int maximumRetryDelay) {
        Preconditions.checkArgument(maximumRetryDelay > 0, "maximumRetryDelay < 0");
        this.maximumRetryDelay = maximumRetryDelay;
    }

// Errors

    /**
     * Handle an exception thrown during an invocation of {@link #performSessionAction performSessionAction()}.
     *
     * <p>
     * The implementation in {@code Session} logs an error message. Subclasses are encouraged to
     * handle errors more gracefully within the context of the associated application.
     *
     * @param e exception thrown during {@link #performSessionAction performSessionAction()}
     */
    protected void reportException(Exception e) {
        this.log.error("exception within session", e);
    }

// Transactions

    /**
     * Associate the current {@link JTransaction} with this instance, if not already associated, while performing the given action.
     *
     * <p>
     * If {@code action} throws an {@link Exception}, it will be caught and handled by {@link #reportException reportException()}
     * and then false returned.
     *
     * <p>
     * This instance must be in {@link SessionMode#JSIMPLEDB}, there must be a {@link JTransaction} open and
     * {@linkplain JTransaction#getCurrent associated with the current thread}, and this instance must not already
     * have a different {@link JTransaction} associated with it (it may already have the same {@link JTransaction}
     * associated with it). The {@link JTransaction} will be left open when this method returns.
     *
     * <p>
     * This method safely handles re-entrant invocation.
     *
     * @param action action to perform
     * @return true if {@code action} completed successfully, false if {@code action} threw an exception
     * @throws IllegalStateException if there is a different open transaction already associated with this instance
     * @throws IllegalStateException if this instance is not in mode {@link SessionMode#JSIMPLEDB}
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean performSessionActionWithCurrentTransaction(Action action) {

        // Sanity check
        Preconditions.checkArgument(action != null, "null action");
        Preconditions.checkArgument(SessionMode.JSIMPLEDB.equals(this.mode), "session is not in JSimpleDB mode");

        // Check for re-entrant invocation, otherwise verify no other transaction is associated
        final Transaction currentTx = JTransaction.getCurrent().getTransaction();
        final KVTransaction currentKVT = currentTx.getKVTransaction();

        // Determine whether to join existing or create new
        final boolean associate;
        if (this.tx == null && this.kvt == null)
            associate = true;
        else if (this.tx == currentTx && this.kvt == currentKVT)
            associate = false;
        else
            throw new IllegalStateException("a different transaction is already open in this session");

        // Perform action
        if (associate) {
            this.tx = currentTx;
            this.kvt = currentKVT;
        }
        try {
            action.run(this);
            return true;
        } catch (Exception e) {
            this.reportException(e);
            return false;
        } finally {
            if (associate) {
                this.tx = null;
                this.kvt = null;
            }
        }
    }

    /**
     * Perform the given action in the context of this session.
     *
     * <p>
     * If {@code action} is a {@link TransactionalAction}, and there is no transaction currently associated with this instance,
     * a new transaction will be created, held open while {@code action} executes, then committed; any transaction options
     * from {@code action} implementing {@link HasTransactionOptions} will be appied. Otherwise, {@code action}
     * is just executed directly.
     *
     * <p>
     * In either case, if {@code action} throws an {@link Exception}, it will be caught and handled by
     * {@link #reportException reportException()} and then false returned.
     *
     * <p>
     * If there is already a transaction currently associated with this instance, it is left open while {@code action}
     * executes and upon return.
     *
     * <p>
     * If {@code action} is a {@link RetryableAction}, and a newly created transaction throws a {@link RetryTransactionException},
     * it will be retried automatically up to the configured {@linkplain #getMaxRetries maximum number of retry attempts}.
     * An exponential back-off algorithm is used: after the first failed attempt, the current thread sleeps for the
     * {@linkplain #getInitialRetryDelay initial retry delay}. After each subsequent failed attempt, the retry delay is doubled,
     * up to the limit imposed by the configured {@linkplain #getMaximumRetryDelay maximum retry delay}.
     *
     * @param action action to perform, possibly within a transaction
     * @return true if {@code action} completed successfully, false if a transaction could not be created
     *  or {@code action} threw an exception
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean performSessionAction(Action action) {

        // Sanity check
        Preconditions.checkArgument(action != null, "null action");

        // Transaction already open or non-transactional action? If so, just use it.
        if (this.kvt != null || !(action instanceof TransactionalAction)) {
            try {
                action.run(this);
                return true;
            } catch (Exception e) {
                this.reportException(e);
                return false;
            }
        }

        // Retry transaction as necessary
        int retryNumber = 0;
        int retryDelay = Math.min(this.maximumRetryDelay, this.initialRetryDelay);
        final boolean shouldRetry = action instanceof RetryableAction;
        final Map<String, ?> options = action instanceof HasTransactionOptions ?
          ((HasTransactionOptions)action).getTransactionOptions() : null;
        while (true) {

            // If this is not the first attempt, sleep for a while before retrying
            if (retryNumber > 0) {
                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException e) {
                    this.reportException(e);
                    return false;
                }
                retryDelay = Math.min(this.maximumRetryDelay, retryDelay * 2);
            }

            // Perform transactional action within a newly created transaction
            try {
                if (!this.openTransaction(options))
                    return false;
                boolean success = false;
                this.rollbackOnly = false;
                try {
                    action.run(this);
                    success = true;
                } finally {
                    success &= this.closeTransaction(success && !this.rollbackOnly);
                }
                return success;
            } catch (Exception e) {
                if (shouldRetry && e instanceof RetryTransactionException && retryNumber++ < this.maxRetries)
                    continue;
                this.reportException(e);
                return false;
            } finally {
                this.tx = null;
            }
        }
    }

    /**
     * Determine whether there is a transaction already associated with this instance.
     *
     * @return true if a transaction is already open
     */
    public boolean isTransactionOpen() {
        return this.kvt != null;
    }

    /**
     * Mark the current transaction to be rolled back.
     *
     * @throws IllegalStateException if there is no transaction associated with this instance
     */
    public void setRollbackOnly() {
        Preconditions.checkState(this.kvt != null, "no transaction is open in this session");
        this.rollbackOnly = true;
    }

    private boolean openTransaction(Map<String, ?> options) {
        try {

            // Sanity check
            Preconditions.checkState(this.tx == null && this.kvt == null, "a transaction is already open in this session");

            // Open transaction at the appropriate level
            switch (this.mode) {
            case KEY_VALUE:
                this.kvt = this.kvdb.createTransaction(options);
                break;
            case CORE_API:
                this.tx = this.db.createTransaction(this.schemaModel, this.schemaVersion, this.allowNewSchema, options);
                this.kvt = this.tx.getKVTransaction();
                break;
            case JSIMPLEDB:
                Preconditions.checkState(!Session.isCurrentJTransaction(),
                  "a JSimpleDB transaction is already open in the current thread");
                final JTransaction jtx = this.jdb.createTransaction(this.allowNewSchema,
                  this.validationMode != null ? this.validationMode : ValidationMode.AUTOMATIC, options);
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
        final SessionMode currentMode = this.mode;
        try {
            Preconditions.checkState(this.tx != null || this.kvt != null, "no transaction");
            switch (currentMode) {
            case JSIMPLEDB:
                if (commit && !this.tx.isRollbackOnly())
                    JTransaction.getCurrent().commit();
                else
                    JTransaction.getCurrent().rollback();
                break;
            case CORE_API:
                if (commit && !this.tx.isRollbackOnly())
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
            switch (currentMode) {
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
     * Callback interface used by {@link Session#performSessionAction Session.performSessionAction()}
     * and {@link Session#performSessionActionWithCurrentTransaction Session.performSessionActionWithCurrentTransaction()}.
     */
    @FunctionalInterface
    public interface Action {

        /**
         * Perform some action using the given {@link Session}.
         *
         * @param session current session
         * @throws Exception if an error occurs
         */
        void run(Session session) throws Exception;
    }

    /**
     * Tagging interface indicating an {@link Action} that requires there to be an open transaction.
     *
     * <p>
     * A new transaction will be created if necessary before this action is executed.
     * If a transaction already exists, it will be (re)used.
     */
    public interface TransactionalAction {
    }

    /**
     * Tagging interface indicating a {@link TransactionalAction} that should be retried if a
     * {@link RetryTransactionException} is thrown.
     */
    public interface RetryableAction extends TransactionalAction {
    }

    /**
     * Interface implemented by {@link TransactionalAction}'s that want to provide custom transaction options.
     *
     * @see org.jsimpledb.kv.KVDatabase#createTransaction(Map) KVDatabase#createTransaction()
     */
    public interface HasTransactionOptions extends TransactionalAction {

        /**
         * Get the options, if any, to be used when creating a new transaction for this action to run in.
         *
         * @return {@link org.jsimpledb.kv.KVDatabase}-specific transaction options, or null for none
         */
        Map<String, ?> getTransactionOptions();
    }
}

