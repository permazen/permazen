
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
import org.jsimpledb.util.ByteUtil;

/**
 * CLI main entry point.
 */
public class Main extends MainClass {

    private static final int KV_MEM = 0;
    private static final int KV_FDB = 1;
    private static final int KV_XML = 2;

    private int kvType = KV_MEM;
    private String fdbClusterFile;
    private File xmlFile;
    private byte[] keyPrefix;
    private File schemaFile;
    private int schemaVersion;
    private boolean newSchema;
    private boolean verbose;
    private boolean readOnly;

    @Override
    public int run(String[] args) throws Exception {

        // Parse command line
        File outputFile = null;
        final ArrayDeque<String> params = new ArrayDeque<String>(Arrays.asList(args));
        while (!params.isEmpty() && params.peekFirst().startsWith("-")) {
            final String option = params.removeFirst();
            if (option.equals("-h") || option.equals("--help")) {
                this.usageMessage();
                return 0;
            } else if (option.equals("-ro") || option.equals("--read-only"))
                this.readOnly = true;
            else if (option.equals("-v") || option.equals("--verbose"))
                this.verbose = true;
            else if (option.equals("--schema")) {
                if (params.isEmpty())
                    this.usageError();
                this.schemaFile = new File(params.removeFirst());
            } else if (option.equals("--version")) {
                if (params.isEmpty())
                    this.usageError();
                final String vstring = params.removeFirst();
                try {
                    this.schemaVersion = Integer.parseInt(vstring);
                    if (this.schemaVersion < 0)
                        throw new IllegalArgumentException("schema version is negative");
                } catch (Exception e) {
                    System.err.println(this.getName() + ": invalid schema version `" + vstring + "': " + e.getMessage());
                    this.usageError();
                }
            } else if (option.equals("--new-schema"))
                this.newSchema = true;
            else if (option.equals("--mem"))
                this.kvType = KV_MEM;
            else if (option.equals("--prefix")) {
                if (params.isEmpty())
                    this.usageError();
                final String value = params.removeFirst();
                try {
                    this.keyPrefix = ByteUtil.parse(value);
                } catch (IllegalArgumentException e) {
                    this.keyPrefix = value.getBytes("UTF-8");
                }
            } else if (option.equals("--fdb")) {
                if (params.isEmpty())
                    this.usageError();
                this.kvType = KV_FDB;
                this.fdbClusterFile = params.removeFirst();
                if (!new File(this.fdbClusterFile).exists())
                    System.err.println(this.getName() + ": file `" + this.fdbClusterFile + "' does not exist");
            } else if (option.equals("--xml")) {
                if (params.isEmpty())
                    this.usageError();
                this.kvType = KV_XML;
                this.xmlFile = new File(params.removeFirst());
                if (!this.xmlFile.exists()) {
                    System.err.println(this.getName() + ": file `" + this.xmlFile + "' does not exist");
                    return 1;
                }
            } else if (option.equals("--"))
                break;
            else {
                System.err.println(this.getName() + ": unknown option `" + option + "'");
                this.usageError();
            }
        }
        switch (params.size()) {
        case 0:
            break;
        default:
            this.usageError();
            return 1;
        }
        if (this.kvType != KV_FDB && this.keyPrefix != null) {
            System.err.println(this.getName() + ": option `--prefix' is only valid in combination with `--fdb'");
            this.usageError();
        }

        // Create KV database
        final KVDatabase kvdb;
        switch (this.kvType) {
        case KV_MEM:
            kvdb = new SimpleKVDatabase();
            break;
        case KV_FDB:
        {
            final FoundationKVDatabase fdb = new FoundationKVDatabase();
            fdb.setClusterFilePath(this.fdbClusterFile);
            fdb.setKeyPrefix(this.keyPrefix);
            fdb.start();
            kvdb = fdb;
            break;
        }
        case KV_XML:
            kvdb = new XMLKVDatabase(this.xmlFile);
            break;
        default:
            throw new RuntimeException("internal error");
        }

        // Set up console
        final Console console = new Console(new Database(kvdb), new FileInputStream(FileDescriptor.in), System.out);
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
        switch (this.kvType) {
        case KV_FDB:
            ((FoundationKVDatabase)kvdb).stop();
            break;
        default:
            break;
        }

        // Done
        return 0;
    }

    protected String getName() {
        return "jsimpledb";
    }

    @Override
    protected void usageMessage() {
        System.err.println("Usage:");
        System.err.println("  " + this.getName() + " [options]");
        System.err.println("Options:");
        System.err.println("  --fdb file        Use FoundationDB with specified cluster file");
        System.err.println("  --mem             Use an empty in-memory database (default)");
        System.err.println("  --prefix prefix   FoundationDB key prefix (hex or string)");
        System.err.println("  --read-only       Disallow database modifications");
        System.err.println("  --new-schema      Allow recording of a new database schema version");
        System.err.println("  --schema file     Load database schema from XML file");
        System.err.println("  --xml file        Use the specified XML flat file database");
        System.err.println("  --version num     Specify database schema version (default highest recorded)");
        System.err.println("  -h, --help        Show this help message");
        System.err.println("  -v, --verbose     Show verbose error messages");
    }

    public static void main(String[] args) throws Exception {
        new Main().doMain(args);
    }
}

