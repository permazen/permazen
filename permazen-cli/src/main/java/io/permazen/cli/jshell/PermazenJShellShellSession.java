
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.jshell;

import com.google.common.base.Preconditions;

import io.permazen.PermazenTransaction;
import io.permazen.cli.HasPermazenSession;
import io.permazen.cli.PermazenShellRequest;
import io.permazen.cli.Session;
import io.permazen.kv.KVDatabase;
import io.permazen.util.ApplicationClassLoader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.dellroad.jct.jshell.JShellShellSession;
import org.dellroad.jct.jshell.LocalContextExecutionControlProvider;

/**
 * A version of the JCT {@link JShellShellSession} that is Permazen aware.
 *
 * <p>
 * Instances are created by {@link PermazenJShellShell}.
 */
public class PermazenJShellShellSession extends JShellShellSession implements HasPermazenSession {

    public static final String RESOURCE_TEMPORARY_FILE_PREFIX = "PermazenJShell-";

    private static final String STARTUP_FILE_RESOURCE = "META-INF/permazen/jshell-startup.jsh";

    private final Session session;

    private volatile boolean runStandardStartup = true;
    private File startupFile;

    private ClassLoader previousContextLoader;
    private boolean extended;

// Constructor

    @SuppressWarnings("this-escape")
    public PermazenJShellShellSession(PermazenJShellShell shell, PermazenShellRequest request) {
        super(shell, request);
        this.session = ((PermazenShellRequest)this.getRequest()).getPermazenSession();

        // Configure local execution
        this.setLocalContextClassLoader(ApplicationClassLoader.getInstance());
    }

    /**
     * Set whether to run the standard Permazen jshell startup script.
     */
    public void setRunStandardStartup(boolean runStandardStartup) {
        this.runStandardStartup = runStandardStartup;
    }

// PermazenJShellShellSession

    /**
     * Get the instance associated with the current thread.
     *
     * <p>
     * This method just returns the result from {@link JShellShellSession#getCurrent} cast to a
     * {@link PermazenJShellShellSession}.
     *
     * @return the instance of this class associated with the current thread, or null if none found
     * @see JShellShellSession#getCurrent
     */
    public static PermazenJShellShellSession getCurrent() {
        try {
            return (PermazenJShellShellSession)JShellShellSession.getCurrent();
        } catch (ClassCastException e) {
            return null;
        }
    }

    // Add startup script to the jshell command line (if found)
    @Override
    protected List<String> modifyJShellParams(List<String> params) {

        // Do the normal stuff
        params = new ArrayList<>(super.modifyJShellParams(params));

        // Use our custom execution control provider, unless another one (other than "localContext", which is what
        // LocalContextExecutionControlProvider inserts) was specified on the "jshell" command line.
        final String providerName = LocalContextExecutionControlProvider.getExecutionFlag(params);
        if (providerName == null || providerName.equals(LocalContextExecutionControlProvider.NAME))
            LocalContextExecutionControlProvider.setExecutionFlag(params, PermazenExecutionControlProvider.NAME);

        // Add our custom startup script
        if (this.runStandardStartup) {
            this.startupFile = PermazenJShellShellSession.resourceToFile(STARTUP_FILE_RESOURCE);
            params.add("--startup");
            params.add(this.startupFile.toString());
        }

        // Done
        return params;
    }

    @Override
    protected int doExecute() throws InterruptedException {
        try {
            return super.doExecute();
        } finally {
            if (this.session.hasTransaction()) {
                this.session.getError().println("Warning: JShell exited with an extended transaction open (aborting)");
                this.session.closeTransaction(false);
            }
            if (this.startupFile != null)
                this.removeIfTemporary(this.startupFile);
        }
    }

// HasPermazenSession

    @Override
    public Session getPermazenSession() {
        return this.session;
    }

// Snippet Transactions

    /**
     * Commit the current extended snippet transaction and open a new non-extended transaction.
     *
     * <p>
     * This method is only intended to be invoked from JShell snippets.
     *
     * @throws IllegalStateException if there is no current snippet transaction
     * @throws IllegalStateException if the current transaction is not an extended transaction
     */
    public void commit() {
        this.endTransaction(true, true);
        this.session.openTransaction(null, null);
    }

    /**
     * Abort the current extended snippet transaction, if any, and open a new non-extended transaction.
     *
     * <p>
     * If there is no current extended snippet transaction, then this method does nothing.
     *
     * <p>
     * This method is only intended to be invoked from JShell snippets.
     */
    public void rollback() {
        if (this.session.hasTransaction() && this.extended) {
            this.endTransaction(false, true);
            this.session.openTransaction(null, null);
        }
    }

    private void endTransaction(boolean commit, boolean expectExtended) {
        Preconditions.checkState(this.session.hasTransaction(), "there is no current snippet transaction");
        if (expectExtended)
            Preconditions.checkState(this.extended, "the current snippet transaction is not an extended transaction");
        else
            Preconditions.checkState(!this.extended, "there is already an extended transaction open");
        this.extended = false;
        this.session.closeTransaction(commit);
    }

    /**
     * Convert the current snippet transaction into an extended transaction.
     *
     * <p>
     * This method is only intended to be invoked from JShell snippets.
     *
     * @throws IllegalStateException if there is no current snippet transaction
     * @throws IllegalStateException if the current transaction is already extended
     */
    public void begin() {
        Preconditions.checkState(this.session.hasTransaction(), "there is no current snippet transaction");
        Preconditions.checkState(!this.extended, "there is already an extended transaction open");
        this.extended = true;
    }

    /**
     * Commit the current snippet transaction and create a new <i>branched</i> extended transaction.
     *
     * <p>
     * This method is only intended to be invoked from JShell snippets.
     *
     * @throws IllegalStateException if there is no current snippet transaction
     * @throws IllegalStateException if the current transaction is already extended
     */
    public void branch() {
        this.branch(null, null);
    }

    /**
     * Commit the current snippet transaction and create a new <i>branched</i> extended transaction with options.
     *
     * <p>
     * This method is only intended to be invoked from JShell snippets.
     *
     * @param openOptions {@link KVDatabase}-specific transaction options for the branch's opening transaction, or null for none
     * @param syncOptions {@link KVDatabase}-specific transaction options for the branch's commit transaction, or null for none
     * @throws IllegalStateException if there is no current snippet transaction
     * @throws IllegalStateException if the current transaction is already extended
     */
    public void branch(Map<String, ?> openOptions, Map<String, ?> syncOptions) {
        this.endTransaction(true, false);
        this.session.openBranchedTransaction(null, openOptions, syncOptions);
        this.extended = true;
    }

    /**
     * Invoked by {@link PermazenExecutionControl#enterContext} to create or join a snippet transaction.
     *
     * @throws IllegalStateException if there's not supposed to be an extended snippet transaction but there's is
     * @throws IllegalStateException if there's supposed to be an extended snippet transaction but there's not
     */
    protected void joinTransaction() {
        if (this.extended) {
            if (this.session.hasTransaction()) {
                this.associateTransactionToCurrentThread();
                return;
            }
            this.session.getError().println("Warning: extended transaction disappeared; opening new non-extended transaction");
            this.extended = false;
        }
        if (this.session.hasTransaction()) {
            this.session.getError().println("Warning: unexpected extended transaction encountered; joining it");
            this.extended = true;
        } else
            this.session.openTransaction(null, null);
        this.associateTransactionToCurrentThread();
    }

    /**
     * Invoked by {@link PermazenExecutionControl#leaveContext} to commit or leave the snippet transaction.
     *
     * @throws IllegalStateException if there is no current snippet transaction
     */
    protected void leaveTransaction(boolean success) {
        this.disassociateTransactionFromCurrentThread();
        if (!this.session.hasTransaction()) {
            // This is likely due to commit() or branch() throwing an exception
            //this.session.getError().println("Warning: no snippet transaction found");
            this.extended = false;
            return;
        }
        if (!this.extended)
            this.session.closeTransaction(success);
    }

    private void associateTransactionToCurrentThread() {
        Optional.of(this.session)
          .map(Session::getTxInfo)
          .filter(txInfo -> txInfo.getMode().hasPermazen())
          .map(Session.TxInfo::getPermazenTransaction)
          .ifPresent(PermazenTransaction::setCurrent);
        this.previousContextLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(ApplicationClassLoader.getInstance());
    }

    private void disassociateTransactionFromCurrentThread() {
        Optional.of(this.session)
          .map(Session::getTxInfo)
          .filter(txInfo -> txInfo.getMode().hasPermazen())
          .ifPresent(txInfo -> PermazenTransaction.setCurrent(null));
        Thread.currentThread().setContextClassLoader(this.previousContextLoader);
        this.previousContextLoader = null;
    }

// Internal Methods

    /**
     * Make a classpath resource available as a {@link File}.
     *
     * <p>
     * If the resource is already a file, that file is returned. Otherwise, a temporary
     * file is created and the file's name will begin with {@link #RESOURCE_TEMPORARY_FILE_PREFIX}.
     *
     * @param resource classpath resource
     * @return {@link File} containing the resource
     * @throws RuntimeException if {@code resource} is not found or inaccessible
     * @throws IllegalArgumentException if {@code resource} is null
     */
    public static File resourceToFile(String resource) {

        // Sanity check
        Preconditions.checkArgument(resource != null, "null resource");

        // First, see if it's already a file
        final ClassLoader loader = ApplicationClassLoader.getInstance();
        try {
            final URL url = loader.getResource(resource);
            if (url != null)
                return new File(url.toURI());
        } catch (URISyntaxException | IllegalArgumentException e) {
            // too bad
        }

        // Read resource into a temporary file
        try (InputStream in = loader.getResourceAsStream(resource)) {
            if (in == null)
                throw new RuntimeException(String.format("resource \"%s\" not found", resource));
            final File file = File.createTempFile(RESOURCE_TEMPORARY_FILE_PREFIX, "tmp");
            try (OutputStream out = new FileOutputStream(file)) {
                in.transferTo(out);
            }
            return file;
        } catch (IOException e) {
            throw new RuntimeException("unexpected error", e);
        }
    }

    private void removeIfTemporary(File file) {
        if (file.getName().startsWith(RESOURCE_TEMPORARY_FILE_PREFIX))
            file.delete();
    }
}
