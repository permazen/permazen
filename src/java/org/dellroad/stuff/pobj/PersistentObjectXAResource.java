
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import javax.validation.ConstraintViolation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * {@link PersistentObject} {@link XAResource} for participation in XA transactions.
 */
class PersistentObjectXAResource<T> implements XAResource {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final PersistentObjectTransactionManager<T> manager;

    PersistentObjectXAResource(PersistentObjectTransactionManager<T> manager) {
        if (manager == null)
            throw new IllegalArgumentException("null manager");
        this.manager = manager;
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {

        // Logging
        if (this.log.isTraceEnabled()) {
            this.log.trace("POBJ XA: start(): xid=" + SimpleXid.toString(xid) + " flags="
              + (flags == TMJOIN ? "TMJOIN" : flags == TMRESUME ? "TMRESUME" : flags == TMNOFLAGS ? "TMNOFLAGS" :
               Integer.toHexString(flags)) + " xaMap=" + this.showXAMap());
        }

        // Get transaction info
        final TxInfo<T> current = this.manager.getCurrentTxInfo();
        final TxInfo<T> info = this.manager.xaMap.get(xid);
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: start(): thread tx=" + current + " xid tx=" + info);

        // Perform start operation
        switch (flags) {
        case TMJOIN:

            // Verify a transaction is associated with the current thread
            if (current == null)
                throw this.buildException(XAException.XAER_OUTSIDE, "no transaction is associated with the current thread");
            this.checkVersion(current);
            break;
        case TMRESUME:

            // Verify transaction is already registered
            if (info == null) {
                throw this.buildException(XAException.XAER_NOTA, "no transaction with XID " + xid
                  + " is registered with the transaction manager");
            }

            // Resume transaction
            try {
                this.manager.doResume(new TxWrapper<T>(info), info);
            } catch (Exception e) {
                throw this.buildException(XAException.XAER_RMERR, "can't resume transaction: " + e.getMessage(), e);
            }
            break;
        case TMNOFLAGS:

            // Join existing transaction or create new one
            if (current != null) {

                // Join existing transaction
                if (this.manager.xaMap.putIfAbsent(xid, current) != null) {
                    throw this.buildException(XAException.XAER_DUPID,
                      "a transaction with XID " + SimpleXid.toString(xid) + " is already registered with the transaction manager");
                }
                if (this.log.isTraceEnabled())
                    this.log.trace("POBJ XA: start(): joined existing transaction " + current);
            } else {

                // Set up tx definition
                final DefaultTransactionDefinition txDef = new DefaultTransactionDefinition();
                txDef.setName(TransactionSynchronizationManager.getCurrentTransactionName());
                txDef.setReadOnly(TransactionSynchronizationManager.isCurrentTransactionReadOnly());

                // Create new transaction
                try {
                    this.manager.doBegin(new TxWrapper<Object>(null), txDef);
                } catch (Exception e) {
                    throw this.buildException(XAException.XAER_RMERR, "can't begin transaction: " + e.getMessage(), e);
                }
                final TxInfo<T> newInfo = this.manager.getCurrentTxInfo();
                if (this.log.isTraceEnabled())
                    this.log.trace("POBJ XA: start(): created new transaction " + newInfo);
                this.manager.xaMap.put(xid, newInfo);
            }
            break;
        default:
            throw this.buildException(XAException.XAER_INVAL, "invalid flags 0x" + Integer.toHexString(flags));
        }
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: start(): new xaMap=" + this.showXAMap());
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        if (this.log.isTraceEnabled()) {
            this.log.trace("POBJ XA: end(): xid=" + SimpleXid.toString(xid) + " flags="
              + (flags == TMSUCCESS ? "TMSUCCESS" : flags == TMFAIL ? "TMFAIL" : flags == TMSUSPEND ? "TMSUSPEND" :
              Integer.toHexString(flags)) + " xaMap=" + this.showXAMap());
        }
        final TxInfo<T> info = this.verifyCurrent(xid);
        this.checkVersion(info);
        switch (flags) {
        case TMSUCCESS:
            break;
        case TMFAIL:
            info.setRollbackOnly(true);
            break;
        case TMSUSPEND:
            try {
                this.manager.doSuspend(new TxWrapper<T>(info));
            } catch (Exception e) {
                throw this.buildException(XAException.XAER_RMERR, "can't resume transaction: " + e.getMessage(), e);
            }
            break;
        default:
            throw this.buildException(XAException.XAER_INVAL, "invalid flags 0x" + Integer.toHexString(flags));
        }
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: end(): new xaMap=" + this.showXAMap());
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: prepare(): xid=" + SimpleXid.toString(xid) + " xaMap=" + this.showXAMap());
        final TxInfo<T> info = this.verifyCurrent(xid);
        this.checkVersion(info);
        if (info.isReadOnly()) {
            this.manager.xaMap.remove(xid);
            this.manager.doCleanupAfterCompletion(new TxWrapper<T>(info));
            if (this.log.isTraceEnabled())
                this.log.trace("POBJ XA: prepare(): " + SimpleXid.toString(xid) + " is read-only");
            return XA_RDONLY;
        }
        try {
            this.createXAFile(xid, info.getSnapshot());
        } catch (PersistentObjectVersionException e) {
            throw this.buildException(XAException.XA_RBTRANSIENT, "persistent object version has changed: " + e.getMessage(), e);
        } catch (PersistentObjectValidationException e) {
            throw this.buildException(XAException.XA_RBINTEGRITY, "invalid persistent object: " + e.getMessage(), e);
        } catch (PersistentObjectException e) {
            throw this.buildException(XAException.XAER_RMERR, "persistent object error: " + e.getMessage(), e);
        }
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: prepare(): new xaMap=" + this.showXAMap());
        return XA_OK;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {

        // Logging
        if (this.log.isTraceEnabled()) {
            this.log.trace("POBJ XA: commit(): xid=" + SimpleXid.toString(xid) + " onePhase="
              + onePhase + " xaMap=" + this.showXAMap());
        }

        // Handle 1PC
        if (onePhase) {
            if (this.prepare(xid) == XA_RDONLY) {
                if (this.log.isTraceEnabled())
                    this.log.trace("POBJ XA: commit() (read-only): new xaMap=" + this.showXAMap());
            }
        }

        // Handle 2PC recovery mode
        final File file = this.getXAFile(xid);
        final TxInfo<T> current = this.manager.getCurrentTxInfo();
        if (current == null) {
            this.log.info("POBJ XA: commit(): no current transaction, assuming recovery for xid=" + xid);
            if (!file.isFile())
                throw this.buildException(XAException.XA_HEURRB, "XID temporary file `" + file + "' invalid or not found");
            try {
                final T root = PersistentObject.read(this.manager.persistentObject.getDelegate(), file, false);
                this.manager.persistentObject.setRoot(root);
            } catch (PersistentObjectValidationException e) {
                throw this.buildException(XAException.XA_RBINTEGRITY, "invalid persistent object: " + e.getMessage(), e);
            } catch (PersistentObjectException e) {
                throw this.buildException(XAException.XAER_RMERR, "persistent object error: " + e.getMessage(), e);
            }
            this.log.info("POBJ XA: commit(): recovery from `" + file + "' successful");
            return;
        }

        // Handle 2PC normal (non-recovery) mode
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: commit(): normal (non-recovery) commit of current transaction");
        final TxInfo<T> info = this.verifyCurrent(xid);
        try {
            this.manager.persistentObject.setRoot(info.getSnapshot().getRoot()/*, info.getSnapshot().getVersion()*/);
        } catch (PersistentObjectException e) {
            throw this.buildException(XAException.XAER_RMERR, "persistent object error: " + e.getMessage(), e);
        } finally {
            file.delete();
            this.manager.xaMap.remove(xid);
            this.manager.doCleanupAfterCompletion(new TxWrapper<T>(info));
        }
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: commit(): new xaMap=" + this.showXAMap());
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: rollback(): xid=" + SimpleXid.toString(xid) + " xaMap=" + this.showXAMap());
        final TxInfo<T> info = this.verifyCurrent(xid);
        this.manager.xaMap.remove(xid);
        this.removeXAFile(xid);
        this.manager.doRollback(new DefaultTransactionStatus(new TxWrapper<T>(info), false, false, false, false, false));
        this.manager.doCleanupAfterCompletion(new TxWrapper<T>(info));
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: rollback(): new xaMap=" + this.showXAMap());
    }

    @Override
    public void forget(Xid xid) throws XAException {
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: forget(): xid=" + xid);
        this.manager.xaMap.remove(xid);
        this.removeXAFile(xid);
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: forget(): new xaMap=" + this.showXAMap());
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        if (this.log.isTraceEnabled()) {
            this.log.trace("POBJ XA: recover(): start=" + ((flag & TMSTARTRSCAN) != 0) + " end=" + ((flag & TMENDRSCAN) != 0)
              + " xaMap=" + this.showXAMap());
        }
        if ((flag & ~(TMSTARTRSCAN | TMENDRSCAN)) != 0)
            throw this.buildException(XAException.XAER_INVAL, "invalid flag 0x" + Integer.toHexString(flag));
        if ((flag & TMSTARTRSCAN) == 0)
            return new Xid[0];
        final Xid[] xids;
        try {
            xids = this.getXAFiles();
        } catch (IOException e) {
            throw this.buildException(XAException.XAER_RMERR, "error scanning for XA recovery files: " + e.getMessage(), e);
        }
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: recover(): new xaMap=" + this.showXAMap());
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: recover(): returning " + Arrays.<Xid>asList(xids));
        return xids;
    }

    @Override
    public boolean isSameRM(XAResource res) throws XAException {
        if (!(res instanceof PersistentObjectXAResource))
            return false;
        final PersistentObjectXAResource<?> that = (PersistentObjectXAResource<?>)res;
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: isSameRM(): this.manager=" + this.manager + " that.manager=" + that.manager);
        return that.manager == this.manager;
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return 0;
    }

    @Override
    public boolean setTransactionTimeout(int timeout) throws XAException {
        return false;
    }

    private TxInfo<T> verifyCurrent(Xid xid) throws XAException {

        // Logging
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: verifying current tx for " + xid);

        // Get current transaction
        final TxInfo<T> current = this.manager.getCurrentTxInfo();
        if (current == null)
            throw this.buildException(XAException.XAER_OUTSIDE, "no transaction is associated with the current thread");

        // Get transaction corresponding to xid
        final TxInfo<T> info = this.manager.xaMap.get(xid);
        if (info == null) {
            throw this.buildException(XAException.XAER_NOTA, "no transaction with XID " + xid
              + " is registered with the transaction manager");
        }

        // Verify they are the same
        if (info != current) {
            throw this.buildException(XAException.XAER_PROTO, "the transaction associated with XID " + xid
              + " does not correspond to the transaction associated with the current thread");
        }

        // Done
        return info;
    }

    private void checkVersion(TxInfo<T> info) throws XAException {
        final long expected = info.getSnapshot().getVersion();
        final long actual = this.manager.persistentObject.getVersion();
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: check version: actual=" + actual + " expected=" + expected);
        if (actual != expected) {
            throw this.buildException(XAException.XA_RBTRANSIENT,
              "persistent object version has changed: " + new PersistentObjectVersionException(actual, expected).getMessage());
        }
    }

    private XAException buildException(int errorCode, String message) {
        return this.buildException(errorCode, message, null);
    }

    private XAException buildException(int errorCode, String message, Throwable cause) {
        if (this.log.isTraceEnabled())
            this.log.trace("POBJ XA: throwing exception: code=" + errorCode + " msg=\"" + message + "\" cause=" + cause);
        final XAException e = new XAException(message);
        if (errorCode != 0)
            e.errorCode = errorCode;
        if (cause != null)
            e.initCause(cause);
        return e;
    }

    private String showXAMap() {
        final StringBuilder buf = new StringBuilder();
        for (Map.Entry<Xid, TxInfo<T>> entry : this.manager.xaMap.entrySet())
            buf.append("\n   ").append(SimpleXid.toString(entry.getKey())).append(" -> ").append(entry.getValue());
        return buf.toString();
    }

    private void createXAFile(Xid xid, PersistentObject<T>.Snapshot snapshot) {

        // Get temporary XA file
        final File file = this.getXAFile(xid);

        // Get info
        final T xaRoot = snapshot.getRoot();
        final long expectedVersion = snapshot.getVersion();
        final long actualVersion = this.manager.persistentObject.getVersion();

        // Check version number
        if (expectedVersion != 0 && actualVersion != expectedVersion)
            throw new PersistentObjectVersionException(actualVersion, expectedVersion);

        // Validate the new root
        final Set<ConstraintViolation<T>> violations = this.manager.persistentObject.getDelegate().validate(xaRoot);
        if (!violations.isEmpty())
            throw new PersistentObjectValidationException(violations);

        // Write file
        boolean success = false;
        try {
            PersistentObject.<T>write(xaRoot, this.manager.persistentObject.getDelegate(), file);       // TODO: fsync
            success = true;
        } finally {
            if (!success)
                file.delete();
        }
    }

    private File getXAFile(Xid xid) {
        final File file = this.manager.persistentObject.getFile();
        return new File(file.getParentFile(), String.format("%s.XA.%s", file.getName(), SimpleXid.toString(xid)));
    }

    private boolean removeXAFile(Xid xid) {
        return this.getXAFile(xid).delete();
    }

    private Xid[] getXAFiles() throws IOException {
        final File file = this.manager.persistentObject.getFile();
        final File dir = file.getParentFile();
        final File[] siblings = dir.listFiles();
        if (siblings == null)
            throw new IOException("can't list the contents of directory `" + dir + "'");
        final ArrayList<Xid> xids = new ArrayList<>();
        for (File sibling : siblings) {
            if (!sibling.isFile())
                continue;
            final Xid xid = SimpleXid.fromString(sibling.getName());
            if (xid != null)
                xids.add(xid);
        }
        return xids.toArray(new Xid[xids.size()]);
    }
}

