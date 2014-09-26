
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

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
 * Transactions are associated with threads; the {@link PersistentObjectTransactionManager} associated with the current thread's
 * transaction is available via {@link #getCurrent()} (or {@link #getCurrent(String)} if there is more than one).
 * </p>
 *
 * <p>
 * The {@code persistentObject} property is required. The {@code readOnlySharedRoot} property is optional and configures whether,
 * during read-only transactions only, {@link #getRoot} returns the {@linkplain PersistentObject#getSharedRoot shared root} object.
 * In this mode, root object graph copies are avoided entirely for read-only transactions, but the application code must be
 * careful to not modify the object graph returned by {@link #getRoot} at any time, either during or after the transaction.
 * </p>
 *
 * <p>
 * This class also provides an {@link XAResource} for use with JTA/XA tranasactions. Once elisted in a JTA transaction,
 * the usage described above using {@link #getRoot} and {@link #setRoot setRoot()} is identical.
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

    final ConcurrentHashMap<Xid, TxInfo<T>> xaMap = new ConcurrentHashMap<>();

    private final ThreadLocal<TxInfo<T>> currentInfo = new ThreadLocal<>();

    private String beanName;
    private boolean readOnlySharedRoot;

// BeanNameAware

    /**
     * Get this instance's assigned bean name.
     */
    public String getBeanName() {
        return this.beanName;
    }

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
     * Get the {@link PersistentObject} that this instance will operate on.
     */
    public PersistentObject<T> getPersistentObject() {
        return this.persistentObject;
    }

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
     * Get whether, during read-only transactions, {@link #getRoot} returns a new copy of the
     * {@link PersistentObject} object graph or the {@linkplain PersistentObject#getSharedRoot shared root}.
     */
    public boolean isReadOnlySharedRoot() {
        return this.readOnlySharedRoot;
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

    /**
     * Get the {@link XAResource} corresponding to this instance.
     */
    public XAResource getXAResource() {
        return new PersistentObjectXAResource<T>(this);
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
        if (manager.logger.isTraceEnabled())
            manager.logger.trace("PersistentObjectTransactionManager.getCurrent(\"" + beanName + "\"): returning " + manager);
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
        final PersistentObjectTransactionManager<T> manager
          = (PersistentObjectTransactionManager<T>)managerMap.values().iterator().next();
        if (manager.logger.isTraceEnabled())
            manager.logger.trace("PersistentObjectTransactionManager.getCurrent(): returning " + manager);
        return manager;
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
     * Determine whether the transaction associated with the current thread is a read-only transaction.
     *
     * @return true if the transaction associated with the current thread is a read-only transaction
     * @throws IllegalStateException if there is no transaction associated with the current thread
     */
    public boolean isReadOnly() {
        final TxInfo<T> info = this.currentInfo.get();
        if (info == null)
            throw new IllegalStateException("there is no transaction associated with the current thread");
        return info.isReadOnly();
    }

    /**
     * Get the snapshot version of the transaction associated with the current thread.
     *
     * @return snapshot version of the transaction associated with the current thread.
     * @throws IllegalStateException if there is no transaction associated with the current thread
     */
    public long getSnapshotVersion() {
        final TxInfo<T> info = this.currentInfo.get();
        if (info == null)
            throw new IllegalStateException("there is no transaction associated with the current thread");
        return info.getSnapshot().getVersion();
    }

    /**
     * Get the root object graph for use in the transaction associated with the current thread.
     *
     * @return root object
     * @throws IllegalStateException if there is no open transaction
     */
    public T getRoot() {
        final TxInfo<T> info = this.currentInfo.get();
        if (info == null)
            throw new IllegalStateException("there is no transaction associated with the current thread");
        return info.getSnapshot().getRoot();
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
     * @throws IllegalStateException if there is no transaction associated with the current thread
     * @throws IllegalArgumentException if {@code root} is null
     */
    public void setRoot(T root) {
        if (root == null)
            throw new IllegalArgumentException("null root");
        final TxInfo<T> info = this.currentInfo.get();
        if (info == null)
            throw new IllegalStateException("there is no transaction associated with the current thread");
        info.setSnapshot(this.persistentObject.new Snapshot(root, info.getSnapshot().getVersion()));
    }

    TxInfo<T> getCurrentTxInfo() {
        return this.currentInfo.get();
    }

// ResourceTransactionManager

    @Override
    public Object getResourceFactory() {
        return this.persistentObject;
    }

// AbstractPlatformTransactionManager

    @Override
    protected Object doGetTransaction() {
        return new TxWrapper<T>(this.currentInfo.get());
    }

    @Override
    protected boolean isExistingTransaction(Object txObj) {
        return ((TxWrapper<?>)txObj).getInfo() != null;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doBegin(Object txObj, TransactionDefinition txDef) {

        // Logging
        if (this.logger.isTraceEnabled())
            this.logger.trace("PersistentObjectTransactionManager.doBegin(): txObj=" + txObj + " txDef=" + txDef);

        // Sanity check
        if (this.currentInfo.get() != null)
            throw new TransactionUsageException("there is already a transaction associated with the current thread");
        if (this.isExistingTransaction(txObj))
            throw new TransactionUsageException("there is already a transaction associated with the given transaction object");
        final TxWrapper<T> tx = (TxWrapper<T>)txObj;

        // Create transaction
        final PersistentObject<T>.Snapshot snapshot;
        try {
            snapshot = txDef.isReadOnly() && this.readOnlySharedRoot ?
              this.persistentObject.getSharedRootSnapshot() : this.persistentObject.getRootSnapshot();
        } catch (PersistentObjectException e) {
            throw new CannotCreateTransactionException("error creating new JSimpleDB transaction", e);
        }

        // Associate transaction with this thread for getCurrent()
        final HashMap<String, PersistentObjectTransactionManager<?>> managerMap
          = PersistentObjectTransactionManager.getManagerMap(true);
        if (managerMap.containsKey(this.beanName)) {
            throw new IllegalStateException("A PersistentObjectTransactionManager named `" + this.beanName
              + "' already has an open transaction in the current thread; all bean names must be distinct");
        }
        managerMap.put(this.beanName, this);

        // Associate new transaction with the current thread
        final TxInfo<T> info = new TxInfo<T>(snapshot, txDef.isReadOnly());
        this.currentInfo.set(info);

        // Set transaction info into the Spring transaction object
        tx.setInfo(info);
    }

    /**
     * Suspend the current transaction.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Object doSuspend(Object txObj) {

        // Logging
        if (this.logger.isTraceEnabled())
            this.logger.trace("PersistentObjectTransactionManager.doSuspend(): txObj=" + txObj);

        // Sanity check
        final TxWrapper<T> tx = (TxWrapper<T>)txObj;
        final TxInfo<T> info = tx.getInfo();
        if (info == null)
            throw new TransactionUsageException("no PersistentObject transaction exists in the given transaction object");
        if (info != this.currentInfo.get())
            throw new TransactionUsageException("the provided transaction object contains the wrong PersistentObject transaction");

        // Suspend it
        if (this.logger.isTraceEnabled())
            this.logger.trace("PersistentObjectTransactionManager.doSuspend(): suspending " + info.getSnapshot());
        this.currentInfo.remove();
        tx.setInfo(null);
        PersistentObjectTransactionManager.getManagerMap(true).remove(this.beanName);

        // Done
        return info;
    }

    /**
     * Resume a previously suspended transaction.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected void doResume(Object txObj, Object suspendedResources) {

        // Logging
        if (this.logger.isTraceEnabled())
            this.logger.trace("PersistentObjectTransactionManager.doResume(): txObj=" + txObj + " res=" + suspendedResources);

        // Sanity check
        if (this.currentInfo.get() != null)
            throw new TransactionUsageException("there is already a transaction associated with the current thread");
        if (this.isExistingTransaction(txObj))
            throw new TransactionUsageException("there is already a transaction associated with the given transaction object");

        // Get previously saved info
        final TxWrapper<T> tx = (TxWrapper<T>)txObj;
        final TxInfo<T> info = (TxInfo<T>)suspendedResources;

        // Resume transaction
        if (this.logger.isTraceEnabled())
            this.logger.trace("PersistentObjectTransactionManager.doSuspend(): resuming " + info.getSnapshot());
        final HashMap<String, PersistentObjectTransactionManager<?>> managerMap
          = PersistentObjectTransactionManager.getManagerMap(true);
        if (managerMap.containsKey(this.beanName)) {
            throw new IllegalStateException("A PersistentObjectTransactionManager named `" + this.beanName
              + "' already has an open transaction in the current thread; all bean names must be distinct");
        }
        managerMap.put(this.beanName, this);
        this.currentInfo.set(info);
        tx.setInfo(info);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void prepareForCommit(DefaultTransactionStatus status) {
        final TxInfo<T> info = ((TxWrapper<T>)status.getTransaction()).getInfo();
        if (this.logger.isTraceEnabled())
            this.logger.trace("PersistentObjectTransactionManager.prepareForCommit(): status=" + status + " info=" + info);
        if (info == null)
            throw new NoTransactionException("no current transaction exists");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doCommit(DefaultTransactionStatus status) {

        // Logging
        if (this.logger.isTraceEnabled())
            this.logger.trace("PersistentObjectTransactionManager.doCommit(): status=" + status);

        // Get transaction
        final TxWrapper<T> tx = (TxWrapper<T>)status.getTransaction();
        final TxInfo<T> info = tx.getInfo();
        if (info == null)
            throw new NoTransactionException("no current transaction exists");
        final PersistentObject<T>.Snapshot snapshot = info.getSnapshot();

        // Commit
        try {
            if (!info.isReadOnly()) {
                if (this.logger.isTraceEnabled())
                    this.logger.trace("PersistentObjectTransactionManager.doCommit(): committing " + info);
                this.persistentObject.setRoot(snapshot.getRoot(), snapshot.getVersion());
            } else
                this.logger.trace("PersistentObjectTransactionManager.doCommit(): not committing read-only " + info);
        } catch (PersistentObjectVersionException e) {
            throw new OptimisticLockingFailureException(null, e);
        } catch (PersistentObjectValidationException e) {
            throw new DataIntegrityViolationException(null, e);
        } catch (PersistentObjectException e) {
            throw new TransactionSystemException("error committing transaction", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doRollback(DefaultTransactionStatus status) {

        // Logging
        if (this.logger.isTraceEnabled())
            this.logger.trace("PersistentObjectTransactionManager.doRollback(): status=" + status);

        // Get transaction
        final TxWrapper<T> tx = (TxWrapper<T>)status.getTransaction();
        final TxInfo<T> info = tx.getInfo();
        if (info == null)
            throw new NoTransactionException("no current transaction exists");
        final PersistentObject<T>.Snapshot snapshot = info.getSnapshot();

        // Rollback
        if (this.logger.isTraceEnabled())
            this.logger.trace("PersistentObjectTransactionManager.doRollback(): rolling back " + info);
        // no action required to rollback a PersistentObject transaction
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doSetRollbackOnly(DefaultTransactionStatus status) {

        // Logging
        if (this.logger.isTraceEnabled())
            this.logger.trace("PersistentObjectTransactionManager.doSetRollbackOnly(): status=" + status);

        // Get transaction
        final TxWrapper<T> tx = (TxWrapper<T>)status.getTransaction();
        final TxInfo<T> info = tx.getInfo();
        if (info == null)
            throw new NoTransactionException("no current transaction exists");

        // Set rollback only
        if (this.logger.isTraceEnabled())
            this.logger.trace("PersistentObjectTransactionManager.doSetRollbackOnly(): setting rollback-only: " + info);
        info.setRollbackOnly(true);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doCleanupAfterCompletion(Object txObj) {

        // Logging
        if (this.logger.isTraceEnabled())
            this.logger.trace("PersistentObjectTransactionManager.doCleanupAfterCompletion(): txObj=" + txObj);

        // Get transaction
        final TxWrapper<T> tx = (TxWrapper<T>)txObj;

        // Clean up
        tx.setInfo(null);
        this.currentInfo.remove();
        final HashMap<String, PersistentObjectTransactionManager<?>> managerMap
          = PersistentObjectTransactionManager.getManagerMap(false);
        if (managerMap != null) {
            managerMap.remove(this.beanName);
            if (managerMap.isEmpty())
                MANAGER_MAP.remove();
        }
    }
}

