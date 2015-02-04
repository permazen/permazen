
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import java.io.File;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

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
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.core.Database;
import org.jsimpledb.parse.func.Function;
import org.jsimpledb.spring.AnnotatedClassScanner;
import org.jsimpledb.util.AbstractMain;

/**
 * GUI main entry point.
 */
public class Main extends AbstractMain implements GUIConfig {

    private static final int DEFAULT_HTTP_PORT = 8080;

    private static Main instance;

    private JSimpleDB jdb;
    private Server server;
    private int port = DEFAULT_HTTP_PORT;
    private URI root;
    private final LinkedHashSet<Class<?>> functionClasses = new LinkedHashSet<>();

    @Override
    protected boolean parseOption(String option, ArrayDeque<String> params) {
        if (option.equals("--root")) {
            if (params.isEmpty())
                this.usageError();
            this.root = new File(params.removeFirst()).toURI();
        } else if (option.equals("--port")) {
            if (params.isEmpty())
                this.usageError();
            this.port = Integer.parseInt(params.removeFirst());
        } else if (option.equals("--func-pkg")) {
            if (params.isEmpty())
                this.usageError();
            this.scanFunctionClasses(params.removeFirst());
        } else
            return false;
        return true;
    }

    private void scanFunctionClasses(String pkgname) {
        for (String className : new AnnotatedClassScanner(Function.class).scanForClasses(pkgname.split("[\\s,]")))
            this.functionClasses.add(this.loadClass(className));
    }

    @Override
    public int run(String[] args) throws Exception {

        // Set singleton
        Main.instance = this;

        // Parse command line
        final ArrayDeque<String> params = new ArrayDeque<String>(Arrays.asList(args));
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
                // ignore
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

            // Create JSimpleDB instance
            this.jdb = this.getJSimpleDBFactory(db).newJSimpleDB();

            // Perform test transaction
            this.performTestTransaction(jdb);

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
    public JSimpleDB getJSimpleDB() {
        return this.jdb;
    }

    @Override
    public boolean isVerbose() {
        return this.verbose;
    }

    @Override
    public Set<Class<?>> getFunctionClasses() {
        return this.functionClasses;
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
          { "--func-pkg package",   "Register @Function-annotated classes found under the specified Java package" },
          { "--port port",          "Specify HTTP port (default " + DEFAULT_HTTP_PORT + ")" },
          { "--root directory",     "Specify GUI install directory" },
        });
    }

    public static void main(String[] args) throws Exception {
        new Main().doMain(args);
    }

    public static Main getInstance() {
        return Main.instance;
    }
}

