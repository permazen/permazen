
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import com.google.common.base.Preconditions;

import io.permazen.Permazen;
import io.permazen.PermazenTransaction;
import io.permazen.ValidationMode;
import io.permazen.core.DatabaseException;
import io.permazen.core.Transaction;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.RetryTransactionException;
import io.permazen.kv.StaleTransactionException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * Permazen implementation of Spring's
 * {@link org.springframework.transaction.PlatformTransactionManager PlatformTransactionManager} interface,
 * allowing methods annotated with Spring's {@link org.springframework.transaction.annotation.Transactional &#64;Transactional}
 * annotation to perform transactions on a {@link Permazen} database.
 *
 * <p>
 * Properly integrates with {@link PermazenTransaction#getCurrent PermazenTransaction.getCurrent()} to participate in
 * existing transactions when appropriate.
 *
 * <p>
 * For some key/value stores, the value of
 * {@link org.springframework.transaction.annotation.Transactional#isolation &#64;Transactional.isolation()}
 * is significant; see the documentation for your specific {@link KVDatabase} for details.
 *
 * @see io.permazen.spring
 */
@SuppressWarnings("serial")
public class PermazenTransactionManager extends AbstractPlatformTransactionManager
  implements ResourceTransactionManager, InitializingBean {

    /**
     * The name of the transaction option passed to
     * {@link KVDatabase#createTransaction(Map) KVDatabase.createTransaction()}
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
     * The configured {@link Permazen} from which transactions are created.
     */
    protected transient Permazen pdb;

    /**
     * The {@link ValidationMode} to use for transactions.
     */
    protected ValidationMode validationMode = DEFAULT_VALIDATION_MODE;

    private boolean validateBeforeCommit;
    private boolean createBranchedTransactions;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.pdb == null)
            throw new Exception("no Permazen configured");
    }

    /**
     * Configure the {@link Permazen} that this instance will operate on.
     *
     * <p>
     * Required property.
     *
     * @param pdb associated database
     */
    public void setPermazen(Permazen pdb) {
        this.pdb = pdb;
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
     * Configure whether to invoke {@link PermazenTransaction#validate} just prior to commit (and prior to any
     * synchronization callbacks). This also causes validation to be performed at the end of each inner
     * transaction that is participating in an outer transaction.
     * If set to false, validation still occurs, but only when the outermost transaction commits.
     *
     * <p>
     * Default false.
     *
     * @param validateBeforeCommit whether to validate after inner transactions
     */
    public void setValidateBeforeCommit(final boolean validateBeforeCommit) {
        this.validateBeforeCommit = validateBeforeCommit;
    }

    /**
     * Configure whether to create branched transactions instead of normal transactions.
     *
     * <p>
     * Default false.
     *
     * @param createBranchedTransactions true to create branched transactions
     * @see Permazen#createBranchedTransaction
     */
    public void setCreateBranchedTransactions(final boolean createBranchedTransactions) {
        this.createBranchedTransactions = createBranchedTransactions;
    }

    @Override
    public Object getResourceFactory() {
        return this.pdb;
    }

    @Override
    protected Object doGetTransaction() {
        return new TxWrapper(this.getCurrent());
    }

    @Override
    protected boolean isExistingTransaction(Object txObj) {
        return ((TxWrapper)txObj).getPermazenTransaction() != null;
    }

    @Override
    protected void doBegin(Object txObj, TransactionDefinition txDef) {

        // Sanity check
        final TxWrapper tx = (TxWrapper)txObj;
        if (tx.getPermazenTransaction() != null)
            throw new TransactionUsageException("there is already a transaction associated with the current thread");

        // Configure transaction options
        final Map<String, Object> options = new HashMap<>();
        this.populateOptions(options, txDef);

        // Create Permazen transaction
        final PermazenTransaction jtx;
        try {
            jtx = this.createTransaction(options);
        } catch (DatabaseException e) {
            throw new CannotCreateTransactionException("error creating new Permazen transaction", e);
        }

        // Configure Permazen transaction and bind to current thread; but if we fail, roll it back
        boolean succeeded = false;
        try {
            this.configureTransaction(jtx, txDef);
            PermazenTransaction.setCurrent(jtx);
            succeeded = true;
        } catch (DatabaseException e) {
            throw new CannotCreateTransactionException("error configuring Permazen transaction", e);
        } finally {
            if (!succeeded) {
                PermazenTransaction.setCurrent(null);
                try {
                    jtx.rollback();
                } catch (DatabaseException e) {
                    // ignore
                }
            }
        }

        // Done
        tx.setPermazenTransaction(jtx);
    }

    /**
     * Populate the given options map for a new transaction.
     *
     * <p>
     * The implementation in {@link PermazenTransactionManager} populates options based on the transaction isolation
     * level as described above.
     *
     * @param options options map
     * @param txDef transaction definition
     */
    protected void populateOptions(Map<String, Object> options, TransactionDefinition txDef) {
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
    }

    /**
     * Create the underlying {@link PermazenTransaction} for a new transaction.
     *
     * <p>
     * The implementation in {@link PermazenTransactionManager} just delegates to
     * {@link Permazen#createTransaction(ValidationMode, Map)} (or
     * {@link Permazen#createBranchedTransaction(ValidationMode, Map, Map)} if so configured) using this instance's
     * configured settings for validation mode and allowing new schema versions.
     *
     * @param options transaction options
     * @return newly created {@link PermazenTransaction}
     * @throws DatabaseException if an error occurs
     */
    protected PermazenTransaction createTransaction(Map<String, Object> options) {
        return this.createBranchedTransactions ?
          this.pdb.createBranchedTransaction(this.validationMode, options, options) :
          this.pdb.createTransaction(this.validationMode, options);
    }

    /**
     * Suspend the current transaction.
     */
    @Override
    protected Object doSuspend(Object txObj) {

        // Sanity check
        final PermazenTransaction jtx = ((TxWrapper)txObj).getPermazenTransaction();
        if (jtx == null)
            throw new TransactionUsageException("no PermazenTransaction exists in the provided transaction object");
        if (jtx != this.getCurrent())
            throw new TransactionUsageException("the provided transaction object contains the wrong PermazenTransaction");

        // Suspend it
        if (this.logger.isTraceEnabled())
            this.logger.trace("suspending current Permazen transaction " + jtx);
        PermazenTransaction.setCurrent(null);

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
        final PermazenTransaction jtx = (PermazenTransaction)suspendedResources;
        if (this.logger.isTraceEnabled())
            this.logger.trace("resuming Permazen transaction " + jtx);
        PermazenTransaction.setCurrent(jtx);
    }

    /**
     * Configure a new transaction.
     *
     * <p>
     * The implementation in {@link PermazenTransactionManager} sets the transaction's timeout and read-only properties.
     *
     * @param jtx transaction to configure
     * @param txDef transaction definition
     * @throws DatabaseException if an error occurs
     */
    protected void configureTransaction(PermazenTransaction jtx, TransactionDefinition txDef) {

        // Set name
        //jtx.setName(txDef.getName());

        // Set read-only
        if (txDef.isReadOnly())
            jtx.getTransaction().setReadOnly(true);

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
        final PermazenTransaction jtx = ((TxWrapper)status.getTransaction()).getPermazenTransaction();
        if (jtx == null)
            throw new NoTransactionException("no current PermazenTransaction exists");

        // Validate
        if (this.validateBeforeCommit) {
            if (this.logger.isTraceEnabled())
                this.logger.trace("triggering validation prior to commit of Permazen transaction " + jtx);
            jtx.validate();
        }
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {

        // Is there a transaction?
        if (!status.hasTransaction())
            return;

        // Get transaction
        final PermazenTransaction jtx = ((TxWrapper)status.getTransaction()).getPermazenTransaction();
        if (jtx == null)
            throw new NoTransactionException("no current PermazenTransaction exists");

        // Commit
        try {
            if (this.logger.isTraceEnabled())
                this.logger.trace("committing Permazen transaction " + jtx);
            jtx.commit();
        } catch (RetryTransactionException e) {
            throw new PessimisticLockingFailureException("transaction must be retried", e);
        } catch (StaleTransactionException e) {
            throw new TransactionTimedOutException("transaction is no longer usable", e);
        } catch (DatabaseException e) {
            throw new TransactionSystemException("error committing transaction", e);
        } finally {
            PermazenTransaction.setCurrent(null);          // transaction is no longer usable
        }
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) {

        // Is there a transaction?
        if (!status.hasTransaction())
            return;

        // Get transaction
        final PermazenTransaction jtx = ((TxWrapper)status.getTransaction()).getPermazenTransaction();
        if (jtx == null)
            throw new NoTransactionException("no current PermazenTransaction exists");

        // Rollback
        try {
            if (this.logger.isTraceEnabled())
                this.logger.trace("rolling back Permazen transaction " + jtx);
            jtx.rollback();
        } catch (StaleTransactionException e) {
            throw new TransactionTimedOutException("transaction is no longer usable", e);
        } catch (DatabaseException e) {
            throw new TransactionSystemException("error committing transaction", e);
        } finally {
            PermazenTransaction.setCurrent(null);          // transaction is no longer usable
        }
    }

    @Override
    protected void doSetRollbackOnly(DefaultTransactionStatus status) {

        // Is there a transaction?
        if (!status.hasTransaction())
            return;

        // Get transaction
        final PermazenTransaction jtx = ((TxWrapper)status.getTransaction()).getPermazenTransaction();
        if (jtx == null)
            throw new NoTransactionException("no current PermazenTransaction exists");

        // Set rollback only
        if (this.logger.isTraceEnabled())
            this.logger.trace("marking Permazen transaction " + jtx + " for rollback-only");
        jtx.getTransaction().setRollbackOnly();
    }

    @Override
    protected void doCleanupAfterCompletion(Object txObj) {
        PermazenTransaction.setCurrent(null);
        super.doCleanupAfterCompletion(txObj);
    }

    @Override
    protected void registerAfterCompletionWithExistingTransaction(Object txObj, List<TransactionSynchronization> synchronizations) {

        // Get transaction
        final PermazenTransaction jtx = ((TxWrapper)txObj).getPermazenTransaction();
        if (jtx == null)
            throw new NoTransactionException("no current PermazenTransaction exists");

        // Add synchronizations
        final Transaction tx = jtx.getTransaction();
        for (TransactionSynchronization synchronization : synchronizations)
            tx.addCallback(new TransactionSynchronizationCallback(synchronization));
    }

    /**
     * Like {@link PermazenTransaction#getCurrent}, but returns null instead of throwing {@link IllegalStateException}.
     *
     * @return the transaction associated with the current thread, or null if there is none
     */
    protected PermazenTransaction getCurrent() {
        return PermazenTransaction.hasCurrent() ? PermazenTransaction.getCurrent() : null;
    }

// TxWrapper

    private static class TxWrapper implements SmartTransactionObject {

        private PermazenTransaction jtx;

        TxWrapper(PermazenTransaction jtx) {
            this.jtx = jtx;
        }

        public PermazenTransaction getPermazenTransaction() {
            return this.jtx;
        }
        public void setPermazenTransaction(PermazenTransaction jtx) {
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
     * {@link Transaction.Callback} interface.
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
            return this.getClass().hashCode() ^ this.synchronization.hashCode();
        }
    }
}
