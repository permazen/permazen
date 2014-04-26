
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.IOException;
import java.io.PrintWriter;

import org.jsimpledb.core.Database;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaModel;

import jline.console.ConsoleReader;

/**
 * Represents one console session.
 */
public class Session {

    private final Database db;
    private final ConsoleReader console;

    private Transaction tx;
    private SchemaModel schemaModel;
    private NameIndex nameIndex;
    private int schemaVersion;
    private boolean allowNewSchema;
    private boolean done;
    private boolean verbose;
    private boolean readOnly;
    private int lineLimit = 16;

    public Session(Database db, ConsoleReader console) {
        this.db = db;
        this.console = console;
    }

    public ConsoleReader getConsole() {
        return this.console;
    }

    public Transaction getTransaction() {
        return this.tx;
    }

    public SchemaModel getSchemaModel() {
        return this.schemaModel;
    }
    public void setSchemaModel(SchemaModel schemaModel) {
        this.schemaModel = schemaModel;
        this.nameIndex = this.schemaModel != null ? new NameIndex(this.schemaModel) : null;
    }

    public NameIndex getNameIndex() {
        return this.nameIndex;
    }

    public int getSchemaVersion() {
        return this.schemaVersion;
    }
    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public int getLineLimit() {
        return this.lineLimit;
    }
    public void setLineLimit(int lineLimit) {
        this.lineLimit = lineLimit;
    }

    public boolean getAllowNewSchema() {
        return this.allowNewSchema;
    }
    public void setAllowNewSchema(boolean allowNewSchema) {
        this.allowNewSchema = allowNewSchema;
    }

    public boolean isVerbose() {
        return this.verbose;
    }
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean isReadOnly() {
        return this.readOnly;
    }
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isDone() {
        return this.done;
    }
    public void setDone(boolean done) {
        this.done = done;
    }

    public void report(Exception e) {
        final String message = e.getLocalizedMessage();
        try {
            this.console.println("ERROR: " + e.getClass().getSimpleName() + (message != null ? ": " + message : ""));
            if (this.verbose)
                e.printStackTrace(new PrintWriter(this.console.getOutput()));
        } catch (IOException ioe) {
            this.setDone(true);
        }
    }

    public void perform(Action action) {
        try {
            final boolean temporaryTransaction = this.tx == null && action instanceof TransactionAction;
            if (temporaryTransaction) {
                if (!this.openTransaction())
                    return;
            }
            boolean success = false;
            try {
                action.run(this);
                success = true;
            } finally {
                if (temporaryTransaction && this.tx != null) {
                    if (success)
                        this.commitTransaction();
                    else
                        this.rollbackTransaction();
                }
            }
        } catch (Exception e) {
            this.report(e);
        }
    }

    public boolean openTransaction() {
        try {
            if (this.tx != null)
                throw new IllegalStateException("a transaction is already open");
            this.tx = this.db.createTransaction(this.schemaModel, this.schemaVersion, this.allowNewSchema);
            final SchemaVersion version = this.tx.getSchemaVersion();
            this.setSchemaModel(version.getSchemaModel());
            this.setSchemaVersion(version.getVersionNumber());
            this.tx.setReadOnly(this.readOnly);
            return true;
        } catch (Exception e) {
            this.tx = null;
            this.report(e);
            return false;
        }
    }

    public boolean commitTransaction() {
        try {
            if (this.tx == null)
                throw new IllegalStateException("no transaction");
            this.tx.commit();
            return true;
        } catch (Exception e) {
            this.report(e);
            return false;
        } finally {
            this.tx = null;
        }
    }

    public boolean rollbackTransaction() {
        try {
            if (this.tx == null)
                throw new IllegalStateException("no transaction");
            this.tx.rollback();
            return true;
        } catch (Exception e) {
            this.report(e);
            return false;
        } finally {
            this.tx = null;
        }
    }
}

