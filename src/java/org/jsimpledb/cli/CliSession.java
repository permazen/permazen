
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.Session;
import org.jsimpledb.cli.cmd.AbstractCommand;
import org.jsimpledb.cli.cmd.CompareSchemasCommand;
import org.jsimpledb.cli.cmd.DeleteSchemaVersionCommand;
import org.jsimpledb.cli.cmd.EvalCommand;
import org.jsimpledb.cli.cmd.HelpCommand;
import org.jsimpledb.cli.cmd.ImportCommand;
import org.jsimpledb.cli.cmd.InfoCommand;
import org.jsimpledb.cli.cmd.LoadCommand;
import org.jsimpledb.cli.cmd.QuitCommand;
import org.jsimpledb.cli.cmd.RaftAddCommand;
import org.jsimpledb.cli.cmd.RaftRemoveCommand;
import org.jsimpledb.cli.cmd.RaftStartElectionCommand;
import org.jsimpledb.cli.cmd.RaftStatusCommand;
import org.jsimpledb.cli.cmd.RaftStepDownCommand;
import org.jsimpledb.cli.cmd.SaveCommand;
import org.jsimpledb.cli.cmd.SetAllowNewSchemaCommand;
import org.jsimpledb.cli.cmd.SetSchemaVersionCommand;
import org.jsimpledb.cli.cmd.SetValidationModeCommand;
import org.jsimpledb.cli.cmd.ShowAllSchemasCommand;
import org.jsimpledb.cli.cmd.ShowSchemaCommand;
import org.jsimpledb.cli.func.DumpFunction;
import org.jsimpledb.cli.func.PrintFunction;
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
     *
     * @return output writer
     */
    public PrintWriter getWriter() {
        return this.writer;
    }

    /**
     * Get the {@link AbstractCommand}s registered with this instance.
     *
     * @return registered commands indexed by name
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
     * Register the standard CLI built-in functions such as {@code print()}, {@code dump()}, etc.
     */
    @Override
    public void registerStandardFunctions() {
        super.registerStandardFunctions();

        // We don't use AnnotatedClassScanner here to avoid having a dependency on the spring classes
        this.registerFunction(DumpFunction.class);
        this.registerFunction(PrintFunction.class);
    }

    /**
     * Register the standard CLI built-in commands.
     */
    public void registerStandardCommands() {

        // We don't use AnnotatedClassScanner here to avoid having a dependency on the spring classes
        this.registerCommand(CompareSchemasCommand.class);
        this.registerCommand(DeleteSchemaVersionCommand.class);
        this.registerCommand(EvalCommand.class);
        this.registerCommand(HelpCommand.class);
        this.registerCommand(ImportCommand.class);
        this.registerCommand(InfoCommand.class);
        this.registerCommand(LoadCommand.class);
        this.registerCommand(QuitCommand.class);
        this.registerCommand(RaftAddCommand.class);
        this.registerCommand(RaftRemoveCommand.class);
        this.registerCommand(RaftStartElectionCommand.class);
        this.registerCommand(RaftStatusCommand.class);
        this.registerCommand(RaftStepDownCommand.class);
        this.registerCommand(SaveCommand.class);
        this.registerCommand(SetAllowNewSchemaCommand.class);
        this.registerCommand(SetSchemaVersionCommand.class);
        this.registerCommand(SetValidationModeCommand.class);
        this.registerCommand(ShowAllSchemasCommand.class);
        this.registerCommand(ShowSchemaCommand.class);
    }

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
     * @return true if {@code action} completed successfully, false if the transaction could not be created
     *  or {@code action} threw an exception
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
     * @return true if {@code action} completed successfully, false if the transaction could not be created
     *  or {@code action} threw an exception
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
         *
         * @param session session with open transaction
         * @throws Exception if an error occurs
         */
        void run(CliSession session) throws Exception;
    }
}

