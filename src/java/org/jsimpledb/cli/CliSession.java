
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli;

import com.google.common.base.Preconditions;

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
import org.jsimpledb.cli.cmd.KVGetCommand;
import org.jsimpledb.cli.cmd.KVLoadCommand;
import org.jsimpledb.cli.cmd.KVPutCommand;
import org.jsimpledb.cli.cmd.KVRemoveCommand;
import org.jsimpledb.cli.cmd.KVSaveCommand;
import org.jsimpledb.cli.cmd.LoadCommand;
import org.jsimpledb.cli.cmd.QuitCommand;
import org.jsimpledb.cli.cmd.RaftAddCommand;
import org.jsimpledb.cli.cmd.RaftFallbackStatusCommand;
import org.jsimpledb.cli.cmd.RaftRemoveCommand;
import org.jsimpledb.cli.cmd.RaftStartElectionCommand;
import org.jsimpledb.cli.cmd.RaftStatusCommand;
import org.jsimpledb.cli.cmd.RaftStepDownCommand;
import org.jsimpledb.cli.cmd.RegisterCommandCommand;
import org.jsimpledb.cli.cmd.RegisterFunctionCommand;
import org.jsimpledb.cli.cmd.SaveCommand;
import org.jsimpledb.cli.cmd.SetAllowNewSchemaCommand;
import org.jsimpledb.cli.cmd.SetSchemaVersionCommand;
import org.jsimpledb.cli.cmd.SetSessionModeCommand;
import org.jsimpledb.cli.cmd.SetValidationModeCommand;
import org.jsimpledb.cli.cmd.SetVerboseCommand;
import org.jsimpledb.cli.cmd.ShowAllSchemasCommand;
import org.jsimpledb.cli.cmd.ShowSchemaCommand;
import org.jsimpledb.cli.func.DumpFunction;
import org.jsimpledb.cli.func.PrintFunction;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;

/**
 * Represents one CLI console session.
 */
public class CliSession extends ParseSession {

    private final Console console;
    private final PrintWriter writer;
    private final TreeMap<String, AbstractCommand> commands = new TreeMap<>();

    private boolean done;
    private boolean verbose;
    private int lineLimit = 16;

// Constructors

    /**
     * Constructor for {@link org.jsimpledb.SessionMode#KEY_VALUE} mode.
     *
     * @param kvdb key/value database
     * @param writer CLI output
     * @param console associated console if any, otherwise null
     * @throws IllegalArgumentException if {@code kvdb} or {@code writer} is null
     */
    public CliSession(KVDatabase kvdb, PrintWriter writer, Console console) {
        super(kvdb);
        Preconditions.checkArgument(writer != null, "null writer");
        this.writer = writer;
        this.console = console;
    }

    /**
     * Constructor for {@link org.jsimpledb.SessionMode#CORE_API} mode.
     *
     * @param db core database
     * @param writer CLI output
     * @param console associated console if any, otherwise null
     * @throws IllegalArgumentException if {@code db} or {@code writer} is null
     */
    public CliSession(Database db, PrintWriter writer, Console console) {
        super(db);
        Preconditions.checkArgument(writer != null, "null writer");
        this.writer = writer;
        this.console = console;
    }

    /**
     * Constructor for {@link org.jsimpledb.SessionMode#JSIMPLEDB} mode.
     *
     * @param jdb database
     * @param writer CLI output
     * @param console associated console if any, otherwise null
     * @throws IllegalArgumentException if {@code jdb} or {@code writer} is null
     */
    public CliSession(JSimpleDB jdb, PrintWriter writer, Console console) {
        super(jdb);
        Preconditions.checkArgument(writer != null, "null writer");
        this.writer = writer;
        this.console = console;
    }

// Accessors

    /**
     * Get the associated {@link Console}, if any.
     *
     * @return associated {@link Console}, or null if there is no console associated with this instance
     */
    public Console getConsole() {
        return this.console;
    }

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
        final Class<?>[] commandClasses = new Class<?>[] {
            CompareSchemasCommand.class,
            DeleteSchemaVersionCommand.class,
            EvalCommand.class,
            HelpCommand.class,
            ImportCommand.class,
            InfoCommand.class,
            KVGetCommand.class,
            KVLoadCommand.class,
            KVPutCommand.class,
            KVRemoveCommand.class,
            KVSaveCommand.class,
            LoadCommand.class,
            QuitCommand.class,
            RaftAddCommand.class,
            RaftFallbackStatusCommand.class,
            RaftRemoveCommand.class,
            RaftStartElectionCommand.class,
            RaftStatusCommand.class,
            RaftStepDownCommand.class,
            RegisterCommandCommand.class,
            RegisterFunctionCommand.class,
            SaveCommand.class,
            SetAllowNewSchemaCommand.class,
            SetSchemaVersionCommand.class,
            SetSessionModeCommand.class,
            SetValidationModeCommand.class,
            SetVerboseCommand.class,
            ShowAllSchemasCommand.class,
            ShowSchemaCommand.class,
        };
        for (Class<?> cl : commandClasses)
            this.registerCommand(cl);
    }

    /**
     * Create an instance of the specified class and register it as a {@link AbstractCommand}.
     * The class must have a public constructor taking either a single {@link CliSession} parameter
     * or no parameters; they will be tried in that order.
     *
     * @param cl command class
     * @return the newly instantiated command
     * @throws IllegalArgumentException if {@code cl} has no suitable constructor
     * @throws IllegalArgumentException if {@code cl} instantiation fails
     * @throws IllegalArgumentException if {@code cl} does not subclass {@link AbstractCommand}
     */
    public AbstractCommand registerCommand(Class<?> cl) {
        if (!AbstractCommand.class.isAssignableFrom(cl))
            throw new IllegalArgumentException(cl + " does not subclass " + AbstractCommand.class.getName());
        final AbstractCommand command = this.instantiate(cl.asSubclass(AbstractCommand.class));
        try {
            command.getSessionModes();
        } catch (UnsupportedOperationException e) {
            throw new IllegalArgumentException(cl + " does not know it's supported session modes", e);
        }
        this.commands.put(command.getName(), command);
        return command;
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
     * Perform the given action in the context of this session.
     *
     * <p>
     * This is a {@link CliSession}-specific overload of
     * {@link Session#performSessionAction Session.performSessionAction()}; see that method for details.
     *
     * @param action action to perform, possibly within a transaction
     * @return true if {@code action} completed successfully, false if a transaction could not be created
     *  or {@code action} threw an exception
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean performCliSessionAction(final Action action) {
        return this.performSessionAction(this.wrap(action));
    }

    /**
     * Associate the current {@link org.jsimpledb.JTransaction} with this instance, if not already associated,
     * while performing the given action.
     *
     * <p>
     * This is a {@link CliSession}-specific overload of
     * {@link Session#performSessionActionWithCurrentTransaction Session.performSessionActionWithCurrentTransaction()};
     * see that method for details.
     *
     * @param action action to perform
     * @return true if {@code action} completed successfully, false if {@code action} threw an exception
     * @throws IllegalStateException if there is a different open transaction already associated with this instance
     * @throws IllegalStateException if this instance is not in mode {@link org.jsimpledb.SessionMode#JSIMPLEDB}
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean performCliSessionActionWithCurrentTransaction(final Action action) {
        return this.performSessionActionWithCurrentTransaction(this.wrap(action));
    }

    private Session.Action wrap(final Action action) {
        return action instanceof TransactionalAction ?
          new Session.TransactionalAction() {
            @Override
            public void run(Session session) throws Exception {
                action.run((CliSession)session);
            }
          } :
          new Session.Action() {
            @Override
            public void run(Session session) throws Exception {
                action.run((CliSession)session);
            }
          };
    }

    /**
     * Callback interface used by {@link CliSession#performCliSessionAction CliSession.performCliSessionAction()}
     * and {@link CliSession#performCliSessionActionWithCurrentTransaction
     *  CliSession.performCliSessionActionWithCurrentTransaction()}.
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

    /**
     * Tagging interface indicating an {@link Action} that requires there to be an open transaction.
     */
    public interface TransactionalAction extends Action {
    }
}

