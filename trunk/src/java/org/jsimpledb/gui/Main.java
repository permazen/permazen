
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
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.util.AbstractMain;

/**
 * GUI main entry point.
 */
public class Main extends AbstractMain {

    private static final int DEFAULT_HTTP_PORT = 8080;

    private static Main instance;

    private Server server;
    private int port = DEFAULT_HTTP_PORT;
    private URI root;

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
        } else
            return false;
        return true;
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
            this.usageError();
            return 1;
        default:
            break;
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

        // Set up model class packages
        final StringBuilder buf = new StringBuilder();
        for (String packageName : params) {
            if (buf.length() > 0)
                buf.append(' ');
            buf.append(packageName);
        }
        System.setProperty("jsimpledb.gui.packages", buf.toString());

        // Start up KV database
        this.startupKVDatabase();

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

        // Shut down KV database
        this.shutdownKVDatabase();

        // Done
        return 0;
    }

    public KVDatabase getKVDatabase() {
        return this.kvdb;
    }

    public boolean isAllowNewSchema() {
        return this.newSchema;
    }

    public int getSchemaVersion() {
        return this.schemaVersion;
    }

    @Override
    protected String getName() {
        return "jsimpledb-gui";
    }

    @Override
    protected void usageMessage() {
        System.err.println("Usage:");
        System.err.println("  " + this.getName() + " [options] java.package ...");
        System.err.println("Options:");
        System.err.println("  --port port       Specify HTTP port (default " + DEFAULT_HTTP_PORT + ")");
        System.err.println("  --root directory  Specify GUI install directory");
        this.outputFlags();
    }

    public static void main(String[] args) throws Exception {
        new Main().doMain(args);
    }

    public static Main getInstance() {
        return Main.instance;
    }
}

