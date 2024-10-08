
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import com.google.common.base.Preconditions;

import io.permazen.Permazen;
import io.permazen.PermazenTransaction;
import io.permazen.ValidationMode;
import io.permazen.core.Database;
import io.permazen.core.Transaction;
import io.permazen.core.TransactionConfig;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.mvcc.BranchedKVTransaction;
import io.permazen.schema.SchemaModel;
import io.permazen.util.ParseException;

import java.io.PrintStream;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.dellroad.jct.core.ConsoleSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds {@link Permazen}-specific state during a CLI session, include the current open transaction, if any.
 *
 * <p>
 * Instances operate in one of three modes; see {@link SessionMode}.
 *
 * <p>
 * Instances are thread safe.
 *
 * @see SessionMode
 */
@ThreadSafe
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

    /**
     * Default error message prefix.
     */
    public static final String DEFAULT_ERROR_MESSAGE_PREFIX = "Error: ";

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ConsoleSession<?, ?> consoleSession;

    private final KVDatabase kvdb;
    private final Permazen pdb;
    private final Database db;

    @GuardedBy("this")
    private SessionMode mode;
    @GuardedBy("this")
    private SchemaModel schemaModel;

    private volatile ValidationMode validationMode = ValidationMode.AUTOMATIC;
    private volatile String databaseDescription;
    private volatile boolean allowNewSchema = true;
    private volatile TransactionConfig.SchemaRemoval schemaRemoval = TransactionConfig.SchemaRemoval.CONFIG_CHANGE;
    private volatile boolean readOnly;

    // Error handling settings
    private volatile boolean verbose;
    private volatile String errorMessagePrefix = DEFAULT_ERROR_MESSAGE_PREFIX;

    // Retry settings
    private volatile int maxRetries = DEFAULT_MAX_RETRIES;
    private volatile int initialRetryDelay = DEFAULT_INITIAL_RETRY_DELAY;
    private volatile int maximumRetryDelay = DEFAULT_MAXIMUM_RETRY_DELAY;

    // The currently associated transaction, if any
    @GuardedBy("this")
    private TxInfo txInfo;

// Constructors

    /**
     * Constructor for {@link SessionMode#KEY_VALUE}.
     *
     * @param consoleSession console session
     * @param kvdb key/value database
     * @throws IllegalArgumentException if either parameter is null
     */
    public Session(ConsoleSession<?, ?> consoleSession, KVDatabase kvdb) {
        this(consoleSession, null, null, Session.notNull(kvdb, "kvdb"));
    }

    /**
     * Constructor for {@link SessionMode#CORE_API}.
     *
     * @param consoleSession console session
     * @param db core database
     * @throws IllegalArgumentException if either parameter is null
     */
    public Session(ConsoleSession<?, ?> consoleSession, Database db) {
        this(consoleSession, null, Session.notNull(db, "db"), null);
    }

    /**
     * Constructor for {@link SessionMode#PERMAZEN}.
     *
     * @param consoleSession console session
     * @param pdb database
     * @throws IllegalArgumentException if either parameter is null
     */
    public Session(ConsoleSession<?, ?> consoleSession, Permazen pdb) {
        this(consoleSession, Session.notNull(pdb, "pdb"), null, null);
    }

    Session(ConsoleSession<?, ?> consoleSession, Permazen pdb, Database db, KVDatabase kvdb) {
        Preconditions.checkArgument(Stream.of(kvdb, db, pdb).filter(Objects::nonNull).count() == 1,
          "exactly one parameter must be non-null");
        this.consoleSession = Session.notNull(consoleSession, "consoleSession");
        final SessionMode initialMode;
        if (pdb != null) {
            this.pdb = pdb;
            this.db = this.pdb.getDatabase();
            this.kvdb = this.db.getKVDatabase();
            initialMode = SessionMode.PERMAZEN;
        } else if (db != null) {
            this.pdb = null;
            this.db = db;
            this.kvdb = this.db.getKVDatabase();
            initialMode = SessionMode.CORE_API;
        } else {
            this.pdb = null;
            this.db = null;
            this.kvdb = kvdb;
            initialMode = SessionMode.KEY_VALUE;
        }
        synchronized (this) {
            this.mode = initialMode;
        }
    }

    private static <T> T notNull(T obj, String name) {
        Preconditions.checkArgument(obj != null, "null " + name);
        return obj;
    }

// Accessors

    /**
     * Get this instance's {@link SessionMode}.
     *
     * @return this instance's {@link SessionMode}.
     */
    public synchronized SessionMode getMode() {
        return this.mode;
    }

    /**
     * Change this instance's {@link SessionMode}.
     *
     * @param mode new {@link SessionMode}
     * @throws IllegalArgumentException if {@code mode} is null
     * @throws IllegalArgumentException if {@code mode} requires a {@link Permazen} (i.e., {@link SessionMode#PERMAZEN}),
     *  or {@link Database} (i.e., {@link SessionMode#CORE_API}) instance, but none was provided at construction
     */
    public synchronized void setMode(SessionMode mode) {
        Preconditions.checkArgument(mode != null, "null mode");
        switch (mode) {
        case KEY_VALUE:
            break;
        case CORE_API:
            Preconditions.checkArgument(this.db != null, "session is not configured with a Core API Database instance");
            break;
        case PERMAZEN:
            Preconditions.checkArgument(this.pdb != null, "session is not configured with a Permazen instance");
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
     * @return the associated {@link Database} or null if this instance is not in {@link SessionMode#PERMAZEN}
     *  or {@link SessionMode#CORE_API}
     */
    public Database getDatabase() {
        return this.db;
    }

    /**
     * Get the associated {@link Permazen}, if any.
     *
     * @return the associated {@link Permazen} or null if this instance is not in {@link SessionMode#PERMAZEN}
     */
    public Permazen getPermazen() {
        return this.pdb;
    }

    /**
     * Get information about the transaction currently associated with this instance.
     *
     * @return info for the transaction associated with this instance
     * @throws IllegalStateException if there is no transaction associated with this instance
     */
    public synchronized TxInfo getTxInfo() {
        Preconditions.checkState(this.txInfo != null, "no transaction is currently associated with this session");
        return this.txInfo;
    }

    /**
     * Get the {@link KVTransaction} currently associated with this instance.
     *
     * <p>
     * Equivalent to {@link #getTxInfo getTxInfo}{@code ().}{@link TxInfo#getKVTransaction getKVTransaction}{@code ()}.
     *
     * @return {@link KVTransaction} associated with this instance
     * @throws IllegalStateException if there is no transaction associated with this session
     */
    public synchronized KVTransaction getKVTransaction() {
        Preconditions.checkState(this.txInfo != null, "no transaction is currently associated with this session");
        return this.txInfo.getKVTransaction();
    }

    /**
     * Get the {@link Transaction} currently associated with this instance.
     *
     * <p>
     * Equivalent to {@link #getTxInfo getTxInfo}{@code ().}{@link TxInfo#getTransaction getTransaction}{@code ()}.
     *
     * @return {@link Transaction} associated with this instance
     * @throws IllegalStateException if there is no transaction associated with this instance
     * @throws IllegalStateException if this instance was not in mode {@link SessionMode#CORE_API} or {@link SessionMode#PERMAZEN}
     *  when the transaction was opened
     */
    public synchronized Transaction getTransaction() {
        Preconditions.checkState(this.txInfo != null, "no transaction is currently associated with this session");
        return this.txInfo.getTransaction();
    }

    /**
     * Get the {@link PermazenTransaction} currently associated with this instance.
     *
     * <p>
     * Equivalent to {@link #getTxInfo getTxInfo}{@code ().}{@link TxInfo#getPermazenTransaction getPermazenTransaction}{@code ()}.
     *
     * @return {@link PermazenTransaction} associated with this instance
     * @throws IllegalStateException if there is no transaction associated with this instance
     * @throws IllegalStateException if this instance was not in mode {@link SessionMode#PERMAZEN} when the transaction was opened
     */
    public synchronized PermazenTransaction getPermazenTransaction() {
        Preconditions.checkState(this.txInfo != null, "no transaction is currently associated with this session");
        return this.txInfo.getPermazenTransaction();
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
    public synchronized SchemaModel getSchemaModel() {
        return this.schemaModel;
    }
    public synchronized void setSchemaModel(SchemaModel schemaModel) {
        this.schemaModel = schemaModel;
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
     * Get the {@link ValidationMode} associated with this instance.
     *
     * <p>
     * Default value is {@link ValidationMode#AUTOMATIC}.
     *
     * <p>
     * This property is only relevant in {@link SessionMode#PERMAZEN}.
     *
     * @return the validation mode used by this session
     */
    public ValidationMode getValidationMode() {
        return this.validationMode;
    }
    public void setValidationMode(ValidationMode validationMode) {
        if (validationMode == null)
            validationMode = ValidationMode.AUTOMATIC;
        this.validationMode = validationMode;
    }

    /**
     * Get whether the recording of new schemas should be allowed.
     * Default value is true.
     *
     * <p>
     * This setting is ignored except in {@link SessionMode#CORE_API}.
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
     * Get the obsolete schema removal policy.
     *
     * <p>
     * Default value is {@link TransactionConfig.SchemaRemoval#CONFIG_CHANGE}
     *
     * <p>
     * This setting is ignored except in {@link SessionMode#CORE_API}.
     *
     * @return if and when to automatically remove unused schemas
     */
    public TransactionConfig.SchemaRemoval getSchemaRemoval() {
        return this.schemaRemoval;
    }
    public void setSchemaRemoval(TransactionConfig.SchemaRemoval schemaRemoval) {
        if (schemaRemoval == null)
            schemaRemoval = TransactionConfig.SchemaRemoval.CONFIG_CHANGE;
        this.schemaRemoval = schemaRemoval;
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
     * Get the maximum number of allowed retries when a {@link RetryableTransactionalAction} is given to
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
     * Get the initial retry delay when a {@link RetryableTransactionalAction} is given to
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
     * Configure the maximum retry delay when a {@link RetryableTransactionalAction} is given to
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

    /**
     * Get the standard output for this CLI session.
     *
     * @return output writer
     */
    public PrintStream getOutput() {
        return this.consoleSession.getOutputStream();
    }

    /**
     * Get the error output for this CLI session.
     *
     * @return error writer
     */
    public PrintStream getError() {
        return this.consoleSession.getErrorStream();
    }

// Errors

    /**
     * Handle an exception thrown during execution of a command.
     *
     * @param e exception that occurred
     */
    protected void reportException(Exception e) {
        final String message = e.getLocalizedMessage();
        if (e instanceof ParseException && message != null)
            this.getError().println(this.getErrorMessagePrefix() + message);
        else {
            this.getError().println(this.getErrorMessagePrefix()
              + e.getClass().getSimpleName() + (message != null ? ": " + message : ""));
        }
        if (this.verbose || this.showStackTrace(e))
            e.printStackTrace(this.getError());
    }

    /**
     * Determine whether a stack trace should be included in error messages by default for the given exception.
     *
     * <p>
     * When {@link #isVerbose} is true, this method is obviated.
     *
     * @param e error
     * @return true to include stack trace
     */
    protected boolean showStackTrace(Exception e) {
        return e instanceof NullPointerException || (e instanceof ParseException && e.getLocalizedMessage() == null);
    }

    /**
     * Get prefix to use when displaying error messages.
     *
     * <p>
     * Default is {@value #DEFAULT_ERROR_MESSAGE_PREFIX}.
     *
     * @return error message prefix
     */
    public String getErrorMessagePrefix() {
        return this.errorMessagePrefix;
    }
    public void setErrorMessagePrefix(String prefix) {
        this.errorMessagePrefix = prefix;
    }

    /**
     * Get whether to always show stack traces with error messages.
     *
     * <p>
     * Default is false.
     *
     * @return whether to always show stack traces
     */
    public boolean isVerbose() {
        return this.verbose;
    }
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

// Transactions

    /**
     * Build a new Core API {@link TransactionConfig} based on this instance.
     *
     * @return core API transaction configuration
     * @throws IllegalStateException if no {@link Database} was provided at construction
     */
    public synchronized TransactionConfig newTransactionConfig() {
        Preconditions.checkState(this.db != null, "session is not configured with a Core API Database instance");
        return TransactionConfig.builder()
          .schemaModel(this.schemaModel)
          .allowNewSchema(this.allowNewSchema)
          .schemaRemoval(this.schemaRemoval)
          .build();
    }

    /**
     * Perform the given action in the context of this session and (optionally) an open transaction.
     *
     * <p>
     * If {@code action} is not a {@link TransactionalAction}, it is just executed with this instance as the parameter.
     *
     * <p>
     * Otherwise, if there is no transaction currently associated with this instance, a new transaction is created,
     * associated with the current thread while {@code action} executes, and then committed; if {@code action} implements
     * {@link TransactionalActionWithOptions} then its transaction options will be appied.
     *
     * <p>
     * Otherwise, the already open transaction associated with this instance is associated with the current thread while
     * {@code action} executes, and left open when this method returns.
     *
     * <p>
     * In any case, if {@code action} throws an {@link Exception}, it will be caught and handled by
     * {@link #reportException reportException()} and then false returned.
     *
     * <p>
     * If {@code action} is a {@link RetryableTransactionalAction}, and a newly created transaction throws a
     * {@link RetryKVTransactionException}, it will be retried automatically up to the configured
     * {@linkplain #getMaxRetries maximum number of retry attempts}.
     * An exponential back-off algorithm is used: after the first failed attempt, the current thread sleeps for the
     * {@linkplain #getInitialRetryDelay initial retry delay}. After each subsequent failed attempt, the retry delay is doubled,
     * up to the limit imposed by the configured {@linkplain #getMaximumRetryDelay maximum retry delay}.
     *
     * @param action action to perform, possibly within a transaction
     * @return true if {@code action} completed successfully, false if a transaction could not be created
     *  or {@code action} threw an exception
     * @throws InterruptedException if the current thread is interrupted
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean performSessionAction(Action action) throws InterruptedException {

        // Sanity check
        Preconditions.checkArgument(action != null, "null action");

        // Non-transactional action?
        if (!(action instanceof TransactionalAction)) {
            try {
                action.run(this);
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                this.reportException(e);
                return false;
            }
            return true;
        }

        // Get required mode
        final TransactionalAction txAction = (TransactionalAction)action;
        final SessionMode txMode = txAction.getTransactionMode(this);
        Preconditions.checkArgument(txMode != null, "null transaction mode");
        final SessionMode sessionMode = this.getMode();
        if (txMode.compareTo(sessionMode) > 0) {
            throw new IllegalArgumentException(String.format(
              "action requires mode %s > %s provided by %s", txMode, sessionMode, "session"));
        }

        // Do we already have a transaction open? Just use it.
        final TxInfo info;
        synchronized (this) {
            info = this.txInfo;
        }
        if (info != null) {
            if (txMode.compareTo(info.getMode()) > 0) {
                throw new IllegalArgumentException(String.format(
                  "action requires mode %s > %s provided by %s", txMode, info.getMode(), "the current transaction"));
            }
            try {
                info.runWithTx(this, action);
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                this.reportException(e);
                return false;
            }
            return true;
        }

        // Get key/value transaction options, if any
        final Map<String, ?> kvoptions = action instanceof TransactionalActionWithOptions ?
          ((TransactionalActionWithOptions)action).getTransactionOptions() : null;

        // Get Core API transaction config (if needed)
        TransactionConfig txConfig0 = null;
        if (txMode.equals(SessionMode.CORE_API)) {
            txConfig0 = txAction.getTransactionConfig(this);
            if (kvoptions != null)
                txConfig0 = txConfig0.copy().kvOptions(kvoptions).build();
        }
        final TransactionConfig txConfig = txConfig0;

        // Create new transaction and retry as necessary
        int retryNumber = 0;
        int retryDelay = Math.min(this.maximumRetryDelay, this.initialRetryDelay);
        while (true) {

            // If this is not the first attempt, sleep for a while before retrying
            if (retryNumber > 0) {
                Thread.sleep(retryDelay);
                retryDelay = Math.min(this.maximumRetryDelay, retryDelay * 2);
            }

            // Perform transactional action within a newly created transaction
            boolean shouldRetry = false;
            try {
                this.openTransaction(txMode,
                  () -> this.kvdb.createTransaction(kvoptions),
                  () -> this.db.createTransaction(txConfig),
                  () -> this.pdb.createTransaction(this.validationMode, kvoptions));
                boolean success = false;
                try {
                    shouldRetry = action instanceof RetryableTransactionalAction;
                    action.run(this);
                    shouldRetry = false;
                    success = true;
                } finally {
                    this.closeTransaction(success);
                }
                return true;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                if (shouldRetry && e instanceof RetryKVTransactionException && retryNumber++ < this.maxRetries)
                    continue;
                this.reportException(e);
                return false;
            }
        }
    }

    /**
     * Determine whether there is a transaction associated with this instance.
     *
     * <p>
     * Equivalent to {@link #getTxInfo getTxInfo() }{@code != null}.
     *
     * @return true if there is an associated transaction
     */
    public synchronized boolean hasTransaction() {
        return this.txInfo != null;
    }

    /**
     * Open a new transaction in the specified mode and associate it with this instance.
     *
     * @param txMode transaction session mode, or null for this session's mode
     * @param kvoptions key/value transaction options, or null for none
     * @return the newly created {@link TxInfo}
     * @throws IllegalStateException if there is already a transaction associated with this instance
     * @throws IllegalStateException if there is already a {@link PermazenTransaction} associated with the current thread
     */
    public TxInfo openTransaction(SessionMode txMode, Map<String, ?> kvoptions) {
        return this.openTransaction(txMode,
          () -> this.kvdb.createTransaction(kvoptions),
          () -> this.db.createTransaction(this.newTransactionConfig().copy().kvOptions(kvoptions).build()),
          () -> this.pdb.createTransaction(this.validationMode, kvoptions));
    }

    /**
     * Open a new branched transaction and associate it with this instance.
     *
     * @param txMode transaction session mode, or null for this session's mode
     * @param openOptions {@link KVDatabase}-specific transaction options for the branch's opening transaction, or null for none
     * @param syncOptions {@link KVDatabase}-specific transaction options for the branch's commit transaction, or null for none
     * @return the newly created {@link TxInfo}
     * @throws IllegalStateException if there is already a transaction associated with this instance
     * @throws IllegalStateException if there is already a {@link PermazenTransaction} associated with the current thread
     */
    public TxInfo openBranchedTransaction(SessionMode txMode, Map<String, ?> openOptions, Map<String, ?> syncOptions) {
        return this.openTransaction(txMode,
          () -> new BranchedKVTransaction(this.kvdb, openOptions, syncOptions),
          () -> this.db.createTransaction(
            new BranchedKVTransaction(this.kvdb, openOptions, syncOptions), this.newTransactionConfig()),
          () -> this.pdb.createBranchedTransaction(this.validationMode, openOptions, syncOptions));
    }

    // Internal transaction opener
    private synchronized TxInfo openTransaction(SessionMode txMode, Supplier<KVTransaction> kvtCreator,
      Supplier<Transaction> txCreator, Supplier<PermazenTransaction> ptxCreator) {
        if (txMode == null)
            txMode = this.mode;
        Preconditions.checkArgument(txMode.compareTo(this.mode) <= 0, "incompatible transaction session mode");
        Preconditions.checkState(this.txInfo == null, "a transaction is already associated with this session");
        TxInfo info = null;
        try {

            // Open transaction at the appropriate level
            switch (txMode) {
            case KEY_VALUE:
                Preconditions.checkArgument(kvtCreator != null, "null kvtCreator");
                info = new TxInfo(kvtCreator.get());
                break;
            case CORE_API:
                Preconditions.checkArgument(txCreator != null, "null txCreator");
                info = new TxInfo(txCreator.get());
                break;
            case PERMAZEN:
                Preconditions.checkArgument(ptxCreator != null, "null ptxCreator");
                info = new TxInfo(ptxCreator.get());
                break;
            default:
                throw new RuntimeException("internal error");
            }

            // Enforce "read-only" setting
            if (this.readOnly) {
                if (txMode.compareTo(SessionMode.CORE_API) >= 0)
                    info.getTransaction().setReadOnly(true);
                info.getKVTransaction().setReadOnly(true);
            }

            // We're good to go
            this.txInfo = info;
        } finally {
            if (this.txInfo == null && info != null)
                info.cleanup();
        }

        // Done
        return this.txInfo;
    }

    /**
     * Closed the transaction previously opened and associated with this instance by {@link #openTransaction openTransaction()}.
     *
     * <p>
     * This essentially does the reverse of {@link #openTransaction openTransaction()}.
     *
     * <p>
     * If {@code commit} is false, and there is no transaction associated with this instance, this method does nothing.
     *
     * @param commit true to commit the transaction, false to roll it back
     * @throws IllegalStateException if {@code commit} is true and there is no transaction associated with this instance
     */
    public synchronized void closeTransaction(boolean commit) {
        if (this.txInfo == null) {
            Preconditions.checkState(!commit, "there is no transaction associated with this session");
            return;
        }
        try {
            switch (this.txInfo.getMode()) {
            case PERMAZEN:
            {
                final PermazenTransaction ptx = this.txInfo.getPermazenTransaction();
                if (commit && !ptx.getTransaction().isRollbackOnly())
                    ptx.commit();
                else
                    ptx.rollback();
                break;
            }
            case CORE_API:
            {
                final Transaction tx = this.txInfo.getTransaction();
                if (commit && !tx.isRollbackOnly())
                    tx.commit();
                else
                    tx.rollback();
                break;
            }
            case KEY_VALUE:
                final KVTransaction kvt = this.txInfo.getKVTransaction();
                if (commit)
                    kvt.commit();
                else
                    kvt.rollback();
                break;
            default:
                throw new RuntimeException("internal error");
            }
        } finally {
            try {
                this.txInfo.cleanup();
            } finally {
                this.txInfo = null;
            }
        }
    }

// Action

    /**
     * Callback interface used by {@link Session#performSessionAction Session.performSessionAction()}
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
     * Extension of {@link Action} that indicates the action requires an open transaction.
     *
     * <p>
     * A new transaction will be created if necessary before this action is executed.
     * If a transaction already exists, it will be (re)used.
     */
    public interface TransactionalAction extends Action {

        /**
         * Get the {@link SessionMode} corresponding to the transaction that should be opened.
         *
         * <p>
         * This must be at or below the mode of the given session.
         *
         * <p>
         * The implementation in {@link TransactionalAction} returns {@code session.}{@link Session#getMode}{@code ()}.
         *
         * @param session current session
         * @return transaction mode
         * @throws IllegalArgumentException if {@code session} is null
         */
        default SessionMode getTransactionMode(Session session) {
            Preconditions.checkArgument(session != null, "null session");
            return session.getMode();
        }

        /**
         * Get the configuration for the new Core API transaction.
         *
         * <p>
         * This method is only invoked when the {@link #getTransactionMode} returns {@link SessionMode#CORE_API}.
         * Note if this {@link Action} also implements {@link TransactionalActionWithOptions}, then a non-null
         * return value from {@link TransactionalActionWithOptions#getTransactionOptions} replaces the options
         * configured in the {@link TransactionConfig} returned here, if any.
         *
         * <p>
         * The implementation in {@link TransactionalAction} returns {@code session.}{@link Session#newTransactionConfig}{@code ()}.
         *
         * @param session current session
         * @return Core API transaction config
         * @throws IllegalArgumentException if {@code session} is null
         */
        default TransactionConfig getTransactionConfig(Session session) {
            Preconditions.checkArgument(session != null, "null session");
            return session.newTransactionConfig();
        }
    }

    /**
     * Extension of {@link TransactionalAction} that indicates the transaction should be retried if a
     * {@link RetryKVTransactionException} is thrown.
     */
    public interface RetryableTransactionalAction extends TransactionalAction {
    }

    /**
     * Extension of {@link TransactionalAction} that indicates the action provides custom transaction options.
     *
     * @see KVDatabase#createTransaction(Map) KVDatabase#createTransaction()
     */
    public interface TransactionalActionWithOptions extends TransactionalAction {

        /**
         * Get the options, if any, to be used when creating a new transaction for this action to run in.
         *
         * @return {@link KVDatabase}-specific transaction options, or null for none
         */
        Map<String, ?> getTransactionOptions();
    }

// TxInfo

    /**
     * Information about the a transaction associated with a {@link Session}.
     */
    public static final class TxInfo {

        private final SessionMode mode;
        private final KVTransaction kvt;
        private final Transaction tx;
        private final PermazenTransaction ptx;

        private TxInfo(KVTransaction kvt) {
            this(SessionMode.KEY_VALUE, kvt, null, null);
        }

        private TxInfo(Transaction tx) {
            this(SessionMode.CORE_API, tx.getKVTransaction(), tx, null);
        }

        private TxInfo(PermazenTransaction ptx) {
            this(SessionMode.PERMAZEN, ptx.getTransaction().getKVTransaction(), ptx.getTransaction(), ptx);
        }

        private TxInfo(SessionMode mode, KVTransaction kvt, Transaction tx, PermazenTransaction ptx) {
            this.mode = mode;
            this.kvt = kvt;
            this.tx = tx;
            this.ptx = ptx;
        }

        /**
         * Get the {@link SessionMode} that the transaction was opened with.
         *
         * @return associated session mode
         */
        public SessionMode getMode() {
            return this.mode;
        }

        /**
         * Get the associated {@link KVTransaction}.
         *
         * @return {@link KVTransaction} associated with this instance, never null
         */
        public KVTransaction getKVTransaction() {
            return this.kvt;
        }

        /**
         * Get the associated {@link Transaction}, if any.
         *
         * @return {@link Transaction} associated with this instance, never null
         * @throws IllegalStateException if the {@link Session} was in {@link SessionMode#KEY_VALUE} mode
         *  when the transaction was opened
         */
        public Transaction getTransaction() {
            Preconditions.checkState(this.tx != null, "Core API Transaction not available in " + this.mode + " mode");
            return this.tx;
        }

        /**
         * Get the associated {@link PermazenTransaction}, if any.
         *
         * @return {@link PermazenTransaction} associated with this instance, never null
         * @throws IllegalStateException if the {@link Session} was in {@link SessionMode#KEY_VALUE} or {@link SessionMode#CORE_API}
         *  mode when the transaction was opened
         */
        public PermazenTransaction getPermazenTransaction() {
            Preconditions.checkState(this.ptx != null, "PermazenTransaction not available in " + this.mode + " mode");
            return this.ptx;
        }

        void runWithTx(Session session, Action action) throws Exception {

            // Sanity check
            Preconditions.checkArgument(session != null, "null session");
            Preconditions.checkArgument(action != null, "null action");

            // Save previous current tx
            PermazenTransaction previous;
            try {
                previous = PermazenTransaction.getCurrent();
            } catch (IllegalStateException e) {
                previous = null;
            }

            // Perform action with our tx then restore previous
            PermazenTransaction.setCurrent(this.ptx);       // note, might be null
            try {
                action.run(session);
            } finally {
                PermazenTransaction.setCurrent(previous);
            }
        }

        void cleanup() {
            if (this.ptx != null)
                this.ptx.rollback();
            if (this.tx != null)
                this.tx.rollback();
            this.kvt.rollback();
        }
    }
}
