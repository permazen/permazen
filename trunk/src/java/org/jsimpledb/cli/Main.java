
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Arrays;

import org.dellroad.stuff.main.MainClass;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.fdb.FoundationKVDatabase;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.kv.simple.XMLKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.util.AbstractMain;
import org.jsimpledb.util.ByteUtil;

/**
 * CLI main entry point.
 */
public class Main extends AbstractMain {

    private File schemaFile;

    @Override
    protected boolean parseOption(String option, ArrayDeque<String> params) {
        if (option.equals("--schema")) {
            if (params.isEmpty())
                this.usageError();
            this.schemaFile = new File(params.removeFirst());
        } else
            return false;
        return true;
    }

    @Override
    public int run(String[] args) throws Exception {

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

        // Start up KV database
        this.startupKVDatabase();

        // Set up console
        final Console console = new Console(new Database(this.kvdb), new FileInputStream(FileDescriptor.in), System.out);
        console.setHistoryFile(new File(new File(System.getProperty("user.home")), ".jsimpledb_history"));
        console.getSession().setReadOnly(this.readOnly);
        console.getSession().setVerbose(this.verbose);
        console.getSession().setSchemaVersion(this.schemaVersion);
        console.getSession().setAllowNewSchema(this.newSchema);
        if (this.schemaFile != null) {
            try {
                final InputStream input = new BufferedInputStream(new FileInputStream(this.schemaFile));
                try {
                    console.getSession().setSchemaModel(SchemaModel.fromXML(input));
                } finally {
                    input.close();
                }
            } catch (Exception e) {
                System.err.println(this.getName() + ": can't load schema from `" + this.schemaFile + "': " + e.getMessage());
                return 1;
            }
        }

        // Run console
        console.run();

        // Shut down KV database
        this.shutdownKVDatabase();

        // Done
        return 0;
    }

    @Override
    protected String getName() {
        return "jsimpledb";
    }

    @Override
    protected void usageMessage() {
        System.err.println("Usage:");
        System.err.println("  " + this.getName() + " [options]");
        System.err.println("Options:");
        System.err.println("  --schema file     Load database schema from XML file");
        this.outputFlags();
    }

    public static void main(String[] args) throws Exception {
        new Main().doMain(args);
    }
}

