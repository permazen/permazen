
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.Session;
import org.jsimpledb.cli.cmd.AbstractCommand;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;

/**
 * Represents one CLI console session.
 */
public class CliSession extends ParseSession {

    private final PrintWriter writer;
    private final TreeMap<String, AbstractCommand> commands = new TreeMap<>();

    private boolean done;
    private boolean verbose;
    private int lineLimit = 16;

// Constructors

    /**
     * Constructor for core level access only.
     *
     * @param db core database
     * @param writer console output
     * @throws IllegalArgumentException if either parameter is null
     */
    public CliSession(Database db, PrintWriter writer) {
        super(db);
        if (writer == null)
            throw new IllegalArgumentException("null writer");
        this.writer = writer;
    }

    /**
     * Constructor for {@link JSimpleDB} level access.
     *
     * @param jdb database
     * @param writer console output
     * @throws IllegalArgumentException if either parameter is null
     */
    public CliSession(JSimpleDB jdb, PrintWriter writer) {
        super(jdb);
        if (writer == null)
            throw new IllegalArgumentException("null writer");
        this.writer = writer;
    }

// Accessors

    /**
     * Get the output {@link PrintWriter} for this CLI session.
     */
    public PrintWriter getWriter() {
        return this.writer;
    }

    /**
     * Get the {@link AbstractCommand}s registered with this instance.
     */
    public SortedMap<String, AbstractCommand> getCommands() {
        return this.commands;
    }

    public int getLineLimit() {
        return this.lineLimit;
    }
    public void setLineLimit(int lineLimit) {
        this.lineLimit = lineLimit;
    }

    public boolean isVerbose() {
        return this.verbose;
    }
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean isDone() {
        return this.done;
    }
    public void setDone(boolean done) {
        this.done = done;
    }

// Command registration

    /**
     * Create an instance of the specified class and register it as a {@link AbstractCommand}.
     * The class must have a public constructor taking either a single {@link CliSession} parameter
     * or no parameters; they will be tried in that order.
     *
     * @param cl command class
     * @throws IllegalArgumentException if {@code cl} has no suitable constructor
     * @throws IllegalArgumentException if {@code cl} instantiation fails
     * @throws IllegalArgumentException if {@code cl} does not subclass {@link AbstractCommand}
     */
    public void registerCommand(Class<?> cl) {
        if (!AbstractCommand.class.isAssignableFrom(cl))
            throw new IllegalArgumentException(cl + " does not subclass " + AbstractCommand.class.getName());
        final AbstractCommand command = this.instantiate(cl.asSubclass(AbstractCommand.class));
        this.commands.put(command.getName(), command);
    }

    private <T> T instantiate(Class<T> cl) {
        Throwable failure;
        try {
            return cl.getConstructor(CliSession.class).newInstance(this);
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

// Errors

    @Override
    protected void reportException(Exception e) {
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

// Action

    /**
     * Perform the given action. This is a convenience method, equivalent to: {@code perform(null, action)}
     *
     * @param action action to perform
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean perform(Action action) {
        return this.perform(null, action);
    }

    /**
     * Perform the given action within the given existing transaction, if any, otherwise within a new transaction.
     * If {@code tx} is not null, it will used and left open when this method returns. Otherwise,
     * if there is already an open transaction associated with this instance, it will be used;
     * otherwise, a new transaction is created for the duration of {@code action} and then committed.
     *
     * <p>
     * If {@code tx} is not null and there is already an open transaction associated with this instance and they
     * are not the same transaction, an {@link IllegalStateException} is thrown.
     * </p>
     *
     * @param tx transaction in which to perform the action, or null to create a new one (if necessary)
     * @param action action to perform
     * @throws IllegalStateException if {@code tx} conflict with the already an open transaction associated with this instance
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean perform(Transaction tx, final Action action) {
        return this.perform(tx, new Session.Action() {
            @Override
            public void run(Session session) throws Exception {
                action.run((CliSession)session);
            }
        });
    }

    /**
     * Callback interface used by {@link CliSession#perform CliSession.perform()}.
     */
    public interface Action {

        /**
         * Perform some action using the given {@link CliSession} while a transaction is open.
         */
        void run(CliSession session) throws Exception;
    }
}

