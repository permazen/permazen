
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.spring;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JTransaction;
import org.jsimpledb.ValidationMode;
import org.jsimpledb.core.DatabaseException;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.StaleTransactionException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.TransactionTimedOutException;
import org.springframework.transaction.TransactionUsageException;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronization;

/**
 * JSimpleDB implementation of Spring's
 * {@link org.springframework.transaction.PlatformTransactionManager PlatformTransactionManager} interface,
 * allowing methods annotated with Spring's {@link org.springframework.transaction.annotation.Transactional &#64;Transactional}
 * annotation to perform transactions on a {@link JSimpleDB} database.
 *
 * <p>
 * Properly integrates with {@link JTransaction#getCurrent JTransaction.getCurrent()} to participate in
 * existing transactions when appropriate.
 *
 * <p>
 * For some key/value stores, the {@link org.springframework.transaction.annotation.Transactional#isolation &#64;isolation()}
 * value is significant; see the documentation for the specific {@link org.jsimpledb.kv.KVDatabase}.
 *
 * @see org.jsimpledb.spring
 */
@SuppressWarnings("serial")
public class JSimpleDBTransactionManager extends AbstractPlatformTransactionManager
  implements ResourceTransactionManager, InitializingBean {

    /**
     * The name of the transaction option passed to
     * {@link org.jsimpledb.kv.KVDatabase#createTransaction(Map) KVDatabase.createTransaction()}
     * containing the {@linkplain TransactionDefinition#getIsolationLevel isolation level} from the
     * transaction definition. Some key/value databases may interpret this option.
     * The value of this option is an {@link Isolation} instance.
     */
    public static final String ISOLATION_OPTION = Isolation.class.getName();

    /**
     * The default {@link ValidationMode} to use for transactions ({@link ValidationMode#AUTOMATIC}).
     */
    public static final ValidationMode DEFAULT_VALIDATION_MODE = ValidationMode.AUTOMATIC;

    /**
     * The configured {@link JSimpleDB} from which transactions are created.
     */
    protected transient JSimpleDB jdb;

    /**
     * Whether a new schema version is allowed. Default true.
     */
    protected boolean allowNewSchema = true;

    /**
     * The {@link ValidationMode} to use for transactions.
     */
    protected ValidationMode validationMode = DEFAULT_VALIDATION_MODE;

    private boolean validateBeforeCommit;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.jdb == null)
            throw new Exception("no JSimpleDB configured");
    }

    /**
     * Configure the {@link JSimpleDB} that this instance will operate on.
     *
     * <p>
     * Required property.
     *
     * @param jdb associated database
     */
    public void setJSimpleDB(JSimpleDB jdb) {
        this.jdb = jdb;
    }

    /**
     * Configure whether a new schema version may be created.
     *
     * <p>
     * Default value is false.
     *
     * @param allowNewSchema whether to allow recording new schema versions
     */
    public void setAllowNewSchema(boolean allowNewSchema) {
        this.allowNewSchema = allowNewSchema;
    }

    /**
     * Configure the {@link ValidationMode} to use for transactions.
     *
     * <p>
     * Default value is {@link ValidationMode#AUTOMATIC}.
     *
     * @param validationMode validation mode for transactions
     */
    public void setValidationMode(ValidationMode validationMode) {
        this.validationMode = validationMode != null ? validationMode : DEFAULT_VALIDATION_MODE;
    }

    /**
     * Configure whether to invoke {@link JTransaction#validate} just prior to commit (and prior to any
     * synchronization callbacks). This also causes validation to be performed at the end of each inner
     * transaction that is participating in an outer transaction.
     * If set to false, validation still occurs, but only when the outermost transaction commits.
     *
     * <p>
     * Default false.
     *
     * @param validateBeforeCommit whether to validate after inner transactions
     */
    public void setValidateBeforeCommit(boolean validateBeforeCommit) {
        this.validateBeforeCommit = validateBeforeCommit;
    }

    @Override
    public Object getResourceFactory() {
        return this.jdb;
    }

    @Override
    protected Object doGetTransaction() {
        return new TxWrapper(this.getCurrent());
    }

    @Override
    protected boolean isExistingTransaction(Object txObj) {
        return ((TxWrapper)txObj).getJTransaction() != null;
    }

    @Override
    protected void doBegin(Object txObj, TransactionDefinition txDef) {

        // Sanity check
        final TxWrapper tx = (TxWrapper)txObj;
        if (tx.getJTransaction() != null)
            throw new TransactionUsageException("there is already a transaction associated with the current thread");

        // Set transaction options
        final Map<String, Object> options = new HashMap<>();
        switch (txDef.getIsolationLevel()) {
        case TransactionDefinition.ISOLATION_READ_UNCOMMITTED:
            options.put(ISOLATION_OPTION, Isolation.READ_UNCOMMITTED);
            break;
        case TransactionDefinition.ISOLATION_READ_COMMITTED:
            options.put(ISOLATION_OPTION, Isolation.READ_COMMITTED);
            break;
        case TransactionDefinition.ISOLATION_REPEATABLE_READ:
            options.put(ISOLATION_OPTION, Isolation.REPEATABLE_READ);
            break;
        case TransactionDefinition.ISOLATION_SERIALIZABLE:
            options.put(ISOLATION_OPTION, Isolation.SERIALIZABLE);
            break;
        case TransactionDefinition.ISOLATION_DEFAULT:
            options.put(ISOLATION_OPTION, Isolation.DEFAULT);
            break;
        default:
            this.logger.warn("unexpected isolation level " + txDef.getIsolationLevel());
            break;
        }

        // Create JSimpleDB transaction
        final JTransaction jtx;
        try {
            jtx = this.createTransaction(options);
        } catch (DatabaseException e) {
            throw new CannotCreateTransactionException("error creating new JSimpleDB transaction", e);
        }

        // Configure JSimpleDB transaction and bind to current thread; but if we fail, roll it back
        boolean succeeded = false;
        try {
            this.configureTransaction(jtx, txDef);
            JTransaction.setCurrent(jtx);
            succeeded = true;
        } catch (DatabaseException e) {
            throw new CannotCreateTransactionException("error configuring JSimpleDB transaction", e);
        } finally {
            if (!succeeded) {
                JTransaction.setCurrent(null);
                try {
                    jtx.rollback();
                } catch (DatabaseException e) {
                    // ignore
                }
            }
        }

        // Done
        tx.setJTransaction(jtx);
    }

    /**
     * Create the underlying {@link JTransaction} for a new transaction.
     *
     * <p>
     * The implementation in {@link JSimpleDBTransactionManager} just delegates to
     * {@link JSimpleDB#createTransaction(boolean, ValidationMode, Map)} using this instance's configured
     * settings for validation mode and allowing new schema versions.
     *
     * @param options transaction options
     * @throws DatabaseException if an error occurs
     */
    protected JTransaction createTransaction(Map<String, Object> options) {
        return this.jdb.createTransaction(this.allowNewSchema, this.validationMode, options);
    }

    /**
     * Suspend the current transaction.
     */
    @Override
    protected Object doSuspend(Object txObj) {

        // Sanity check
        final JTransaction jtx = ((TxWrapper)txObj).getJTransaction();
        if (jtx == null)
            throw new TransactionUsageException("no JTransaction exists in the provided transaction object");
        if (jtx != this.getCurrent())
            throw new TransactionUsageException("the provided transaction object contains the wrong JTransaction");

        // Suspend it
        if (this.logger.isTraceEnabled())
            this.logger.trace("suspending current JSimpleDB transaction " + jtx);
        JTransaction.setCurrent(null);

        // Done
        return jtx;
    }

    /**
     * Resume a previously suspended transaction.
     */
    @Override
    protected void doResume(Object txObj, Object suspendedResources) {

        // Sanity check
        if (this.getCurrent() != null)
            throw new TransactionUsageException("there is already a transaction associated with the current thread");

        // Resume transaction
        final JTransaction jtx = (JTransaction)suspendedResources;
        if (this.logger.isTraceEnabled())
            this.logger.trace("resuming JSimpleDB transaction " + jtx);
        JTransaction.setCurrent(jtx);
    }

    /**
     * Configure a new transaction.
     *
     * <p>
     * The implementation in {@link JSimpleDBTransactionManager} sets the transaction's timeout and read-only properties.
     *
     * @param jtx transaction to configure
     * @param txDef transaction definition
     * @throws DatabaseException if an error occurs
     */
    protected void configureTransaction(JTransaction jtx, TransactionDefinition txDef) {

        // Set name
        //jtx.setName(txDef.getName());

        // Set read-only
        jtx.getTransaction().setReadOnly(txDef.isReadOnly());

        // Set lock timeout
        final int timeout = this.determineTimeout(txDef);
        if (timeout != TransactionDefinition.TIMEOUT_DEFAULT) {
            try {
                jtx.getTransaction().setTimeout(timeout * 1000L);
            } catch (UnsupportedOperationException e) {
                if (this.logger.isDebugEnabled())
                    this.logger.debug("setting non-default timeout of " + timeout + "sec not supported by underlying transaction");
            }
        }
    }

    @Override
    protected void prepareForCommit(DefaultTransactionStatus status) {

        // Is there a transaction?
        if (!status.hasTransaction())
            return;

        // Get transaction
        final JTransaction jtx = ((TxWrapper)status.getTransaction()).getJTransaction();
        if (jtx == null)
            throw new NoTransactionException("no current JTransaction exists");

        // Validate
        if (this.validateBeforeCommit) {
            if (this.logger.isTraceEnabled())
                this.logger.trace("triggering validation prior to commit of JSimpleDB transaction " + jtx);
            jtx.validate();
        }
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {

        // Is there a transaction?
        if (!status.hasTransaction())
            return;

        // Get transaction
        final JTransaction jtx = ((TxWrapper)status.getTransaction()).getJTransaction();
        if (jtx == null)
            throw new NoTransactionException("no current JTransaction exists");

        // Commit
        try {
            if (this.logger.isTraceEnabled())
                this.logger.trace("committing JSimpleDB transaction " + jtx);
            jtx.commit();
        } catch (RetryTransactionException e) {
            throw new PessimisticLockingFailureException("transaction must be retried", e);
        } catch (StaleTransactionException e) {
            throw new TransactionTimedOutException("transaction is no longer usable", e);
        } catch (DatabaseException e) {
            throw new TransactionSystemException("error committing transaction", e);
        } finally {
            JTransaction.setCurrent(null);          // transaction is no longer usable
        }
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) {

        // Is there a transaction?
        if (!status.hasTransaction())
            return;

        // Get transaction
        final JTransaction jtx = ((TxWrapper)status.getTransaction()).getJTransaction();
        if (jtx == null)
            throw new NoTransactionException("no current JTransaction exists");

        // Rollback
        try {
            if (this.logger.isTraceEnabled())
                this.logger.trace("rolling back JSimpleDB transaction " + jtx);
            jtx.rollback();
        } catch (StaleTransactionException e) {
            throw new TransactionTimedOutException("transaction is no longer usable", e);
        } catch (DatabaseException e) {
            throw new TransactionSystemException("error committing transaction", e);
        } finally {
            JTransaction.setCurrent(null);          // transaction is no longer usable
        }
    }

    @Override
    protected void doSetRollbackOnly(DefaultTransactionStatus status) {

        // Is there a transaction?
        if (!status.hasTransaction())
            return;

        // Get transaction
        final JTransaction jtx = ((TxWrapper)status.getTransaction()).getJTransaction();
        if (jtx == null)
            throw new NoTransactionException("no current JTransaction exists");

        // Set rollback only
        if (this.logger.isTraceEnabled())
            this.logger.trace("marking JSimpleDB transaction " + jtx + " for rollback-only");
        jtx.getTransaction().setRollbackOnly();
    }

    @Override
    protected void doCleanupAfterCompletion(Object txObj) {
        JTransaction.setCurrent(null);
    }

    @Override
    protected void registerAfterCompletionWithExistingTransaction(Object txObj, List<TransactionSynchronization> synchronizations) {

        // Get transaction
        final JTransaction jtx = ((TxWrapper)txObj).getJTransaction();
        if (jtx == null)
            throw new NoTransactionException("no current JTransaction exists");

        // Add synchronizations
        final Transaction tx = jtx.getTransaction();
        for (TransactionSynchronization synchronization : synchronizations)
            tx.addCallback(new TransactionSynchronizationCallback(synchronization));
    }

    /**
     * Like {@link JTransaction#getCurrent}, but returns null instead of throwing {@link IllegalStateException}.
     *
     * @return the transaction associated with the current thread, or null if there is none
     */
    protected JTransaction getCurrent() {
        try {
            return JTransaction.getCurrent();
        } catch (IllegalStateException e) {
            return null;
        }
    }

// TxWrapper

    private static class TxWrapper implements SmartTransactionObject {

        private JTransaction jtx;

        TxWrapper(JTransaction jtx) {
            this.jtx = jtx;
        }

        public JTransaction getJTransaction() {
            return this.jtx;
        }
        public void setJTransaction(JTransaction jtx) {
            this.jtx = jtx;
        }

        @Override
        public boolean isRollbackOnly() {
            return this.jtx != null && this.jtx.getTransaction().isRollbackOnly();
        }

        @Override
        public void flush() {
        }
    }

// SynchronizationCallback

    /**
     * Adapter class that wraps a Spring {@link TransactionSynchronization} in the
     * {@link org.jsimpledb.core.Transaction.Callback} interface.
     */
    public static class TransactionSynchronizationCallback implements Transaction.Callback {

        protected final TransactionSynchronization synchronization;

        /**
         * Constructor.
         *
         * @param synchronization transaction callback
         * @throws IllegalArgumentException if {@code synchronization} is null
         */
        public TransactionSynchronizationCallback(TransactionSynchronization synchronization) {
            Preconditions.checkArgument(synchronization != null, "null synchronization");
            this.synchronization = synchronization;
        }

        @Override
        public void beforeCommit(boolean readOnly) {
            this.synchronization.beforeCommit(readOnly);
        }

        @Override
        public void beforeCompletion() {
            this.synchronization.beforeCompletion();
        }

        @Override
        public void afterCommit() {
            this.synchronization.afterCommit();
        }

        @Override
        public void afterCompletion(boolean committed) {
            this.synchronization.afterCompletion(committed ?
              TransactionSynchronization.STATUS_COMMITTED : TransactionSynchronization.STATUS_ROLLED_BACK);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final TransactionSynchronizationCallback that = (TransactionSynchronizationCallback)obj;
            return this.synchronization.equals(that.synchronization);
        }

        @Override
        public int hashCode() {
            return this.synchronization.hashCode();
        }
    }
}
