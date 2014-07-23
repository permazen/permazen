
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JTransaction;
import org.jsimpledb.ValidationMode;
import org.jsimpledb.cli.cmd.Command;
import org.jsimpledb.cli.func.Function;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.cli.parse.expr.Value;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaModel;

/**
 * Represents one console session.
 */
public class Session {

    private final JSimpleDB jdb;
    private final Database db;
    private final PrintWriter writer;
    private final LinkedHashSet<String> imports = new LinkedHashSet<>();
    private final TreeMap<String, Command> commands = new TreeMap<>();
    private final TreeMap<String, Function> functions = new TreeMap<>();
    private final TreeMap<String, Value> variables = new TreeMap<>();

    private Transaction tx;
    private SchemaModel schemaModel;
    private ValidationMode validationMode;
    private NameIndex nameIndex;
    private int schemaVersion;
    private boolean allowNewSchema;
    private boolean done;
    private boolean verbose;
    private boolean readOnly;
    private int lineLimit = 16;

// Constructors

    /**
     * Constructor for core level access only.
     *
     * @param db core database
     * @param writer console output
     * @throws IllegalArgumentException if either parameter is null
     */
    public Session(Database db, PrintWriter writer) {
        this(null, db, writer);
    }

    /**
     * Constructor for {@link JSimpleDB} level access.
     *
     * @param jdb database
     * @param writer console output
     * @throws IllegalArgumentException if either parameter is null
     */
    public Session(JSimpleDB jdb, PrintWriter writer) {
        this(jdb, jdb.getDatabase(), writer);
    }

    private Session(JSimpleDB jdb, Database db, PrintWriter writer) {
        if (db == null)
            throw new IllegalArgumentException("null db");
        if (writer == null)
            throw new IllegalArgumentException("null writer");
        this.jdb = jdb;
        this.db = db;
        this.writer = writer;
        this.imports.add("java.lang.*");
    }

// Accessors

    /**
     * Get the associated {@link JSimpleDB}, if any.
     *
     * @return the associated {@link JSimpleDB} or null if there is none
     */
    public JSimpleDB getJSimpleDB() {
        return this.jdb;
    }

    /**
     * Determine if this instance has an associated {@link JSimpleDB}.
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

    public PrintWriter getWriter() {
        return this.writer;
    }

    public Set<String> getImports() {
        return this.imports;
    }

    public SortedMap<String, Command> getCommands() {
        return this.commands;
    }

    public SortedMap<String, Function> getFunctions() {
        return this.functions;
    }

    public SortedMap<String, Value> getVars() {
        return this.variables;
    }

    public Transaction getTransaction() {
        if (this.tx == null)
            throw new IllegalStateException("no transaction associated with session");
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
        return this.nameIndex != null ? this.nameIndex : new NameIndex(new SchemaModel());
    }

    public int getSchemaVersion() {
        return this.schemaVersion;
    }
    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public ValidationMode getValidationMode() {
        return this.validationMode;
    }
    public void setValidationMode(ValidationMode validationMode) {
        this.validationMode = validationMode;
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

// Command and Function registration

    /**
     * Create an instance of the specified class and register it as a {@link Command}.
     * The class must have a public constructor taking either a single {@link Session} parameter
     * or no parameters; they will be tried in that order.
     *
     * @param cl command class
     * @throws IllegalArgumentException if {@code cl} has no suitable constructor
     * @throws IllegalArgumentException if {@code cl} instantiation fails
     * @throws IllegalArgumentException if {@code cl} does not subclass {@link Command}
     */
    public void registerCommand(Class<?> cl) {
        if (!Command.class.isAssignableFrom(cl))
            throw new IllegalArgumentException(cl + " does not subclass " + Command.class.getName());
        final Command command = this.instantiate(cl.asSubclass(Command.class));
        this.commands.put(command.getName(), command);
    }

    /**
     * Create an instance of the specified class and register it as a {@link Function}.
     * as appropriate. The class must have a public constructor taking either a single {@link Session} parameter
     * or no parameters; they will be tried in that order.
     *
     * @param cl function class
     * @throws IllegalArgumentException if {@code cl} has no suitable constructor
     * @throws IllegalArgumentException if {@code cl} instantiation fails
     * @throws IllegalArgumentException if {@code cl} does not subclass {@link Command}
     */
    public void registerFunction(Class<?> cl) {
        if (!Function.class.isAssignableFrom(cl))
            throw new IllegalArgumentException(cl + " does not subclass " + Function.class.getName());
        final Function function = this.instantiate(cl.asSubclass(Function.class));
        this.functions.put(function.getName(), function);
    }

    private <T> T instantiate(Class<T> cl) {
        Throwable failure;
        try {
            return cl.getConstructor(Session.class).newInstance(this);
        } catch (NoSuchMethodException e) {
            try {
                return cl.getConstructor().newInstance();
            } catch (NoSuchMethodException e2) {
                throw new IllegalArgumentException("no suitable constructor found in class " + cl.getName());
            } catch (Exception e2) {
                failure = e2;
            }
        } catch (Exception e) {
            failure = e;
        }
        if (failure instanceof InvocationTargetException)
            failure = failure.getCause();
        throw new IllegalArgumentException("unable to instantiate class " + cl.getName() + ": " + failure, failure);
    }

// Class name resolution

    public Class<?> resolveClass(final String name) {
        final int firstDot = name.indexOf('.');
        final String firstPart = firstDot != -1 ? name.substring(0, firstDot - 1) : name;
        final ArrayList<String> packages = new ArrayList<>(this.imports.size() + 1);
        packages.add(null);
        packages.addAll(this.imports);
        for (String pkg : packages) {

            // Get absolute class name
            String className;
            if (pkg == null)
                className = name;
            else if (pkg.endsWith(".*"))
                className = pkg.substring(0, pkg.length() - 1) + name;
            else {
                if (!firstPart.equals(pkg.substring(pkg.lastIndexOf('.') + 1, pkg.length() - 2)))
                    continue;
                className = pkg.substring(0, pkg.length() - 2 - firstPart.length()) + name;
            }

            // Try package vs. nested classes
            while (true) {
                try {
                    return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
                } catch (ClassNotFoundException e) {
                    // not found
                }
                final int lastDot = className.lastIndexOf('.');
                if (lastDot == -1)
                    break;
                className = className.substring(0, lastDot) + "$" + className.substring(lastDot + 1);
            }
        }
        return null;
    }

// Errors

    public void report(Exception e) {
        final String message = e.getLocalizedMessage();
        if (e instanceof ParseException && message != null)
            this.writer.println("Error: " + message);
        else
            this.writer.println("Error: " + e.getClass().getSimpleName() + (message != null ? ": " + message : ""));
        if (this.verbose || this.showStackTrace(e))
            e.printStackTrace(this.writer);
    }

    protected boolean showStackTrace(Exception e) {
        return e instanceof NullPointerException || (e instanceof ParseException && e.getLocalizedMessage() == null);
    }

// Transactions

    /**
     * Perform the given action within a transaction.
     */
    public boolean perform(Action action) {
        try {
            final boolean newTransaction = this.tx == null;
            if (newTransaction) {
                if (!this.openTransaction())
                    return false;
            }
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
            this.report(e);
            return false;
        }
    }

    private boolean openTransaction() {
        try {
            if (this.tx != null)
                throw new IllegalStateException("a transaction is already open");
            if (this.jdb != null) {
                boolean exists = true;
                try {
                    JTransaction.getCurrent();
                } catch (IllegalStateException e) {
                    exists = false;
                }
                if (exists)
                    throw new IllegalStateException("a transaction is already open");
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
            this.report(e);
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
            this.report(e);
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
            this.report(e);
            return false;
        } finally {
            this.tx = null;
            if (this.jdb != null)
                JTransaction.setCurrent(null);
        }
    }
}

