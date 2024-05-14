
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.jshell;

import com.google.common.base.Preconditions;

import io.permazen.cli.HasPermazenSession;
import io.permazen.cli.PermazenShellRequest;
import io.permazen.cli.Session;
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

    private volatile boolean runStandardStartup = true;
    private File startupFile;

// Constructor

    @SuppressWarnings("this-escape")
    public PermazenJShellShellSession(PermazenJShellShell shell, PermazenShellRequest request) {
        super(shell, request);

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
            if (this.startupFile != null)
                this.removeIfTemporary(this.startupFile);
        }
    }

// HasPermazenSession

    @Override
    public Session getPermazenSession() {
        return ((PermazenShellRequest)this.getRequest()).getPermazenSession();
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
