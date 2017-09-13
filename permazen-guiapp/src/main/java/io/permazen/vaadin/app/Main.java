
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin.app;

import com.google.common.base.Preconditions;

import io.permazen.Permazen;
import io.permazen.app.AbstractMain;
import io.permazen.core.Database;

import java.io.File;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Arrays;

import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.plus.webapp.EnvConfiguration;
import org.eclipse.jetty.plus.webapp.PlusConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.FragmentConfiguration;
import org.eclipse.jetty.webapp.MetaInfConfiguration;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.webapp.WebInfConfiguration;
import org.eclipse.jetty.webapp.WebXmlConfiguration;

/**
 * GUI main entry point.
 */
public class Main extends AbstractMain implements GUIConfig {

    private static final int DEFAULT_HTTP_PORT = 8080;

    private static Main instance;

    private Permazen jdb;
    private Server server;
    private int port = DEFAULT_HTTP_PORT;
    private URI root;

    @Override
    protected boolean parseOption(String option, ArrayDeque<String> params) {
        switch (option) {
        case "--root":
            if (params.isEmpty())
                this.usageError();
            this.root = new File(params.removeFirst()).toURI();
            break;
        case "--port":
            if (params.isEmpty())
                this.usageError();
            this.port = Integer.parseInt(params.removeFirst());
            break;
        default:
            return false;
        }
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {

        // Set singleton
        synchronized (Main.class) {
            Preconditions.checkState(Main.instance == null, "only one instance of this class is expected");
            Main.instance = this;
        }

        // Parse command line
        final ArrayDeque<String> params = new ArrayDeque<>(Arrays.asList(args));
        final int result = this.parseOptions(params);
        if (result != -1)
            return result;
        switch (params.size()) {
        case 0:
            break;
        default:
            this.usageError();
            return 1;
        }
        if (this.schemaClasses == null) {
            System.err.println(this.getName() + ": no schema classes defined; please specify using `--pkg'");
            return 1;
        }

        // Try to infer root directory from classpath
        if (this.root == null) {
            try {
                URI uri = this.getClass().getResource("/WEB-INF/web.xml").toURI();
                if (uri.toString().startsWith("jar:"))
                    this.root = new URI(uri.toString().substring(0, uri.toString().lastIndexOf('!') + 2));
                else
                    this.root = this.getClass().getResource("/WEB-INF/web.xml").toURI().resolve("..");
            } catch (Exception e) {
                if (this.isVerbose())
                    e.printStackTrace(System.err);
            }
            if (this.root == null) {
                System.err.println(this.getName() + ": can't determine install directory; please specify `--root dir'");
                return 1;
            }
        }
        this.log.debug("using root directory " + this.root);

        // Set up database
        final Database db = this.startupKVDatabase();
        try {

            // Create Permazen instance
            this.jdb = this.getPermazenFactory(db).newPermazen();

            // Create web server with Spring application context
            this.server = new Server(this.port);
            final WebAppContext context = new WebAppContext();
            context.setBaseResource(Resource.newResource(this.root));
            context.setConfigurations(new Configuration[] {
                                new AnnotationConfiguration(),
                                new WebXmlConfiguration(),
                                new WebInfConfiguration(),
                                new PlusConfiguration(),
                                new MetaInfConfiguration(),
                                new FragmentConfiguration(),
                                new EnvConfiguration() });
            context.setContextPath("/");
            context.setParentLoaderPriority(true);
            this.server.setHandler(context);

            // Start server
            this.server.start();

            // Wait for server to stop
            this.server.join();

            // Done
            return 0;
        } finally {

            // Shut down KV database
            this.shutdownKVDatabase();
        }
    }

    @Override
    public Permazen getPermazen() {
        return this.jdb;
    }

    @Override
    public boolean isVerbose() {
        return this.verbose;
    }

    @Override
    protected String getName() {
        return "jsimpledb-gui";
    }

    @Override
    protected void usageMessage() {
        System.err.println("Usage:");
        System.err.println("  " + this.getName() + " --pkg package [options]");
        System.err.println("Options:");
        this.outputFlags(new String[][] {
          { "--port port",          "Specify HTTP port (default " + DEFAULT_HTTP_PORT + ")" },
          { "--root directory",     "Specify GUI install directory" },
        });
    }

    public static void main(String[] args) throws Exception {
        new Main().doMain(args);
    }

    public static synchronized Main getInstance() {
        return Main.instance;
    }
}
