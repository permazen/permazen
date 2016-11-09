
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli;

import com.google.common.base.Preconditions;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.Session;
import org.jsimpledb.cli.cmd.Command;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.util.ImplementationsReader;

/**
 * Represents one CLI console session.
 */
public class CliSession extends ParseSession {

    /**
     * Classpath XML file resource describing available {@link Command}s: {@value #CLI_COMMANDS_DESCRIPTOR_RESOURCE}.
     *
     * <p>
     * Example:
     * <blockquote><pre>
     *  &lt;cli-command-implementations&gt;
     *      &lt;cli-command-implementation class="com.example.MyCliCommand"/&gt;
     *  &lt;/cli-command-implementations&gt;
     * </pre></blockquote>
     *
     * <p>
     * Instances must have a public constructor taking either zero parameters or one {@link CliSession} parameter.
     *
     * @see #loadCommandsFromClasspath
     */
    public static final String CLI_COMMANDS_DESCRIPTOR_RESOURCE = "META-INF/jsimpledb/cli-command-implementations.xml";

    private final Console console;
    private final PrintWriter writer;
    private final TreeMap<String, Command> commands = new TreeMap<>();

    private boolean done;
    private boolean verbose;
    private int lineLimit = 16;
    private String errorMessagePrefix = "Error: ";

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
     * Get the {@link Command}s registered with this instance.
     *
     * @return registered commands indexed by name
     */
    public SortedMap<String, Command> getCommands() {
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
     * Scan the classpath for {@link Command}s and register them.
     *
     * @see #CLI_COMMANDS_DESCRIPTOR_RESOURCE
     */
    public void loadCommandsFromClasspath() {
        final ImplementationsReader reader = new ImplementationsReader("cli-command");
        final ArrayList<Object[]> paramLists = new ArrayList<>(2);
        paramLists.add(new Object[] { this });
        paramLists.add(new Object[0]);
        reader.setConstructorParameterLists(paramLists);
        for (Command command : reader.findImplementations(Command.class, CLI_COMMANDS_DESCRIPTOR_RESOURCE))
            this.registerCommand(command);
    }

    /**
     * Register the given {@link Command}.
     *
     * <p>
     * Any existing {@link Command} with the same name will be replaced.
     *
     * @param command new command
     * @throws IllegalArgumentException if {@code command} is null
     */
    public void registerCommand(Command command) {
        Preconditions.checkArgument(command != null, "null command");
        this.commands.put(command.getName(), command);
    }

// Errors

    @Override
    protected void reportException(Exception e) {
        final String message = e.getLocalizedMessage();
        if (e instanceof ParseException && message != null)
            this.writer.println(this.getErrorMessagePrefix() + message);
        else {
            this.writer.println(this.getErrorMessagePrefix()
              + e.getClass().getSimpleName() + (message != null ? ": " + message : ""));
        }
        if (this.verbose || this.showStackTrace(e))
            e.printStackTrace(this.writer);
    }

    protected boolean showStackTrace(Exception e) {
        return e instanceof NullPointerException || (e instanceof ParseException && e.getLocalizedMessage() == null);
    }

    /**
     * Get prefix for error messages.
     *
     * <p>
     * Default prefix is {@code "Error: "}.
     *
     * @return error message prefix
     */
    public String getErrorMessagePrefix() {
        return this.errorMessagePrefix;
    }

    /**
     * Set prefix for error messages.
     *
     * @param prefix error message prefix
     */
    public void setErrorMessagePrefix(String prefix) {
        this.errorMessagePrefix = prefix;
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

    private WrapperAction wrap(final Action action) {
        return action instanceof Session.RetryableAction ?
           new RetryableWrapperAction(action) :
         action instanceof Session.TransactionalAction ?
           new TransactionalWrapperAction(action) : new WrapperAction(action);
    }

    private static class WrapperAction implements Session.Action {

        protected final Action action;

        WrapperAction(Action action) {
            this.action = action;
        }

        @Override
        public void run(Session session) throws Exception {
           this.action.run((CliSession)session);
        }
    }

    private static class TransactionalWrapperAction extends WrapperAction
      implements Session.TransactionalAction, Session.HasTransactionOptions {

        TransactionalWrapperAction(Action action) {
            super(action);
        }

        @Override
        public Map<String, ?> getTransactionOptions() {
            return this.action instanceof HasTransactionOptions ?
              ((HasTransactionOptions)this.action).getTransactionOptions() : null;
        }
    }

    private static class RetryableWrapperAction extends TransactionalWrapperAction implements Session.RetryableAction {

        RetryableWrapperAction(Action action) {
            super(action);
        }
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
     *
     * @deprecated Implement {@link org.jsimpledb.Session.TransactionalAction} instead of this interface
     */
    @Deprecated
    public interface TransactionalAction extends Session.TransactionalAction {
    }
}

