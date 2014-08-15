
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.util.HashMap;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.TransactionUsageException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;

/**
 * {@link PersistentObject} implementation of Spring's
 * {@link org.springframework.transaction.PlatformTransactionManager PlatformTransactionManager} interface,
 * allowing methods annotated with Spring's {@link org.springframework.transaction.annotation.Transactional &#64;Transactional}
 * annotation to perform transactions on {@link PersistentObject}s.
 *
 * <p>
 * During a transaction, the transaction's root object is available via the method {@link #getRoot}, and may be replaced entirely
 * via {@link #setRoot setRoot()}. When the transaction completes, the transaction's root object will be automatically
 * written back to the {@link PersistentObject} via {@link PersistentObject#setRoot(Object, long) PersistentObject.setRoot()}
 * (unless the transaction was read-only). During commit, the {@link PersistentObject} version number is verified, and
 * if another update has occurred since the transaction was opened, a {@link PersistentObjectVersionException} is thrown
 * (consider using {@link org.dellroad.stuff.spring.RetryTransaction &#64;RetryTransaction} for automatic retry in this case).
 * </p>
 *
 * <p>
 * The {@code persistentObject} property is required. The {@code readOnlySharedRoot} property is optional and configures whether,
 * during read-only transactions only, {@link #getRoot} returns the {@linkplain PersistentObject#getSharedRoot shared root} object.
 * In this mode, root object graph copies are avoided entirely for read-only transactions, but the application code must be
 * careful to not modify the object graph returned by {@link #getRoot} at any time, either during or after the transaction.
 * </p>
 *
 * @param <T> type of the root object
 * @see PersistentObject
 * @see PersistentObject#getSharedRoot
 */
@SuppressWarnings("serial")
public class PersistentObjectTransactionManager<T> extends AbstractPlatformTransactionManager
  implements BeanNameAware, ResourceTransactionManager, InitializingBean {

    private static final ThreadLocal<HashMap<String, PersistentObjectTransactionManager<?>>> MANAGER_MAP = new ThreadLocal<>();

    /**
     * The configured {@link PersistentObject} from which transactions are created.
     */
    protected transient PersistentObject<T> persistentObject;

    private final ThreadLocal<PersistentObject<T>.Snapshot> current = new ThreadLocal<>();

    private String beanName;
    private boolean readOnlySharedRoot;

// BeanNameAware

    @Override
    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

// InitializingBean

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.persistentObject == null)
            throw new Exception("no PersistentObject configured");
        if (this.beanName == null)
            throw new Exception("no bean name configured");
    }

// Properties

    /**
     * Configure the {@link PersistentObject} that this instance will operate on.
     *
     * <p>
     * Required property.
     * </p>
     */
    public void setPersistentObject(PersistentObject<T> persistentObject) {
        this.persistentObject = persistentObject;
    }

    /**
     * Configure whether, during read-only transactions, {@link #getRoot} returns a new copy of the
     * {@link PersistentObject} object graph or the {@linkplain PersistentObject#getSharedRoot shared root}.
     *
     * <p>
     * Default value is false.
     * </p>
     */
    public void setReadOnlySharedRoot(boolean readOnlySharedRoot) {
        this.readOnlySharedRoot = readOnlySharedRoot;
    }

// Current Instance

    /**
     * Get the instance associated with an open transaction in the current thread and having the given bean name.
     *
     * @param beanName bean name assigned to the desired instance
     * @throws IllegalArgumentException if {@code name} is null
     * @throws IllegalStateException if the current thread is not running within a transaction managed by
     *  a {@link PersistentObjectTransactionManager} assigned the given name
     */
    @SuppressWarnings("unchecked")
    public static <T> PersistentObjectTransactionManager<T> getCurrent(String beanName) {
        if (beanName == null)
            throw new IllegalArgumentException("null beanName");
        final HashMap<String, PersistentObjectTransactionManager<?>> managerMap
          = PersistentObjectTransactionManager.getManagerMap(false);
        final PersistentObjectTransactionManager<?> manager = managerMap != null ? managerMap.get(beanName) : null;
        if (manager == null) {
            throw new IllegalStateException("no PersistentObjectTransactionManager named `" + beanName
              + "' has an open transaction in the current thread");
        }
        return (PersistentObjectTransactionManager<T>)manager;
    }

    /**
     * Get the (unique) instance associated with an open transaction in the current thread.
     *
     * <p>
     * This is a convenience method for the common case where there is only one instance associated with the current thread.
     * </p>
     *
     * @throws IllegalStateException if the current thread is not running within a
     *  {@link PersistentObjectTransactionManager} transaction
     * @throws IllegalStateException there is more than one {@link PersistentObjectTransactionManager} transaction
     *  open in the current thread
     */
    @SuppressWarnings("unchecked")
    public static <T> PersistentObjectTransactionManager<T> getCurrent() {
        final HashMap<String, PersistentObjectTransactionManager<?>> managerMap
          = PersistentObjectTransactionManager.getManagerMap(false);
        if (managerMap == null || managerMap.isEmpty()) {
            throw new IllegalStateException("there are no PersistentObjectTransactionManager transactions open"
              + " in the current thread");
        }
        if (managerMap.size() > 1) {
            throw new IllegalStateException("there are multiple PersistentObjectTransactionManager transactions open"
              + " in the current thread: " + managerMap.keySet() + "; invoke getCurrent() with explicitly specified bean name");
        }
        return (PersistentObjectTransactionManager<T>)managerMap.values().iterator().next();
    }

    private static HashMap<String, PersistentObjectTransactionManager<?>> getManagerMap(boolean create) {
        HashMap<String, PersistentObjectTransactionManager<?>> managerMap = MANAGER_MAP.get();
        if (managerMap == null && create) {
            managerMap = new HashMap<String, PersistentObjectTransactionManager<?>>();
            MANAGER_MAP.set(managerMap);
        }
        return managerMap;
    }

// Root Object Access

    /**
     * Get the root object graph for use in the transaction associated with the current thread.
     *
     * @return root object
     * @throws IllegalStateException if there is no open transaction
     */
    public T getRoot() {
        final PersistentObject<T>.Snapshot snapshot = this.current.get();
        if (snapshot == null)
            throw new IllegalStateException("there is no transaction associated with the current thread");
        return snapshot.getRoot();
    }

    /**
     * Change the root object graph to be committed in the transaction associated with the current thread.
     * Subsequent calls to {@link #getRoot getRoot} will return the new object.
     *
     * <p>
     * This method may be invoked during read-only transactions (as before, the root will not actually be committed).
     * </p>
     *
     * @param root new root object
     * @throws IllegalStateException if there is no open transaction
     */
    public void setRoot(T root) {
        final PersistentObject<T>.Snapshot snapshot = this.current.get();
        if (snapshot == null)
            throw new IllegalStateException("there is no transaction associated with the current thread");
        this.current.set(this.persistentObject.new Snapshot(root, snapshot.getVersion()));
    }

// ResourceTransactionManager

    @Override
    public Object getResourceFactory() {
        return this.persistentObject;
    }

// AbstractPlatformTransactionManager

    @Override
    protected Object doGetTransaction() {
        return new TxWrapper<T>(this.current.get());
    }

    @Override
    protected boolean isExistingTransaction(Object txObj) {
        return ((TxWrapper<?>)txObj).getSnapshot() != null;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doBegin(Object txObj, TransactionDefinition txDef) {

        // Sanity check
        final TxWrapper<T> tx = (TxWrapper<T>)txObj;
        if (tx.getSnapshot() != null)
            throw new TransactionUsageException("there is already a transaction associated with the current thread");

        // Create transaction
        final PersistentObject<T>.Snapshot snapshot;
        try {
            snapshot = txDef.isReadOnly() && this.readOnlySharedRoot ?
              this.persistentObject.getSharedRootSnapshot() : this.persistentObject.getRootSnapshot();
        } catch (PersistentObjectException e) {
            throw new CannotCreateTransactionException("error creating new JSimpleDB transaction", e);
        }

        // Associate transaction with thread for getCurrent()
        final HashMap<String, PersistentObjectTransactionManager<?>> managerMap
          = PersistentObjectTransactionManager.getManagerMap(true);
        if (managerMap.containsKey(this.beanName)) {
            throw new IllegalStateException("A PersistentObjectTransactionManager named `" + this.beanName
              + "' already has an open transaction in the current thread; all bean names must be distinct");
        }
        managerMap.put(this.beanName, this);

        // Done
        this.current.set(snapshot);
        tx.setSnapshot(snapshot);
    }

    /**
     * Suspend the current transaction.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Object doSuspend(Object txObj) {

        // Sanity check
        final PersistentObject<T>.Snapshot snapshot = ((TxWrapper<T>)txObj).getSnapshot();
        if (snapshot == null)
            throw new TransactionUsageException("no PersistentObject snapshot exists in the provided transaction object");
        if (snapshot != this.current.get())
            throw new TransactionUsageException("the provided transaction object contains the wrong PersistentObject snapshot");

        // Suspend it
        if (this.logger.isTraceEnabled())
            this.logger.trace("suspending current PersistentObject transaction" + snapshot);
        this.current.remove();
        PersistentObjectTransactionManager.getManagerMap(true).remove(this.beanName);

        // Done
        return snapshot;
    }

    /**
     * Resume a previously suspended transaction.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected void doResume(Object txObj, Object suspendedResources) {

        // Sanity check
        if (this.current.get() != null)
            throw new TransactionUsageException("there is already a transaction associated with the current thread");

        // Resume transaction
        final PersistentObject<T>.Snapshot snapshot = (PersistentObject<T>.Snapshot)suspendedResources;
        if (this.logger.isTraceEnabled())
            this.logger.trace("resuming PersistentObject transaction " + snapshot);
        final HashMap<String, PersistentObjectTransactionManager<?>> managerMap
          = PersistentObjectTransactionManager.getManagerMap(true);
        if (managerMap.containsKey(this.beanName)) {
            throw new IllegalStateException("A PersistentObjectTransactionManager named `" + this.beanName
              + "' already has an open transaction in the current thread; all bean names must be distinct");
        }
        managerMap.put(this.beanName, this);
        this.current.set(snapshot);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void prepareForCommit(DefaultTransactionStatus status) {
        final PersistentObject<T>.Snapshot snapshot = ((TxWrapper<T>)status.getTransaction()).getSnapshot();
        if (snapshot == null)
            throw new NoTransactionException("no current transaction exists");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doCommit(DefaultTransactionStatus status) {

        // Get transaction
        final PersistentObject<T>.Snapshot snapshot = ((TxWrapper<T>)status.getTransaction()).getSnapshot();
        if (snapshot == null)
            throw new NoTransactionException("no current transaction exists");

        // Commit
        try {
            if (this.logger.isTraceEnabled())
                this.logger.trace("committing PersistentObject transaction " + snapshot);
            this.persistentObject.setRoot(snapshot.getRoot(), snapshot.getVersion());
        } catch (PersistentObjectVersionException e) {
            throw new OptimisticLockingFailureException(null, e);
        } catch (PersistentObjectValidationException e) {
            throw new DataIntegrityViolationException(null, e);
        } catch (PersistentObjectException e) {
            throw new TransactionSystemException("error committing transaction", e);
        } finally {
            this.current.remove();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doRollback(DefaultTransactionStatus status) {

        // Get transaction
        final PersistentObject<T>.Snapshot snapshot = ((TxWrapper<T>)status.getTransaction()).getSnapshot();
        if (snapshot == null)
            throw new NoTransactionException("no current transaction exists");

        // Rollback
        try {
            if (this.logger.isTraceEnabled())
                this.logger.trace("rolling back PersistentObject transaction " + snapshot);
            // no action required to rollback a PersistentObject transaction
        } finally {
            this.current.remove();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doSetRollbackOnly(DefaultTransactionStatus status) {

        // Get transaction
        final TxWrapper<T> tx = (TxWrapper<T>)status.getTransaction();
        final PersistentObject<T>.Snapshot snapshot = tx.getSnapshot();
        if (snapshot == null)
            throw new NoTransactionException("no current transaction exists");

        // Set rollback only
        if (this.logger.isTraceEnabled())
            this.logger.trace("marking PersistentObject transaction " + snapshot + " for rollback-only");
        tx.setRollbackOnly(true);
    }

    @Override
    protected void doCleanupAfterCompletion(Object txObj) {
        this.current.remove();
        final HashMap<String, PersistentObjectTransactionManager<?>> managerMap
          = PersistentObjectTransactionManager.getManagerMap(false);
        if (managerMap != null) {
            managerMap.remove(this.beanName);
            if (managerMap.isEmpty())
                MANAGER_MAP.remove();
        }
        managerMap.put(this.beanName, this);
    }

// TxWrapper

    private static class TxWrapper<S> implements SmartTransactionObject {

        private PersistentObject<S>.Snapshot snapshot;
        private boolean rollbackOnly;

        TxWrapper(PersistentObject<S>.Snapshot snapshot) {
            this.snapshot = snapshot;
        }

        public PersistentObject<S>.Snapshot getSnapshot() {
            return this.snapshot;
        }
        public void setSnapshot(PersistentObject<S>.Snapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public boolean isRollbackOnly() {
            return this.rollbackOnly;
        }
        public void setRollbackOnly(boolean rollbackOnly) {
            this.rollbackOnly = rollbackOnly;
        }

        @Override
        public void flush() {
        }
    }
}

