
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayDeque;

import org.dellroad.stuff.main.MainClass;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.fdb.FoundationKVDatabase;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.kv.simple.XMLKVDatabase;

/**
 * CLI main entry point.
 */
public abstract class AbstractMain extends MainClass {

    protected static final int KV_MEM = 0;
    protected static final int KV_FDB = 1;
    protected static final int KV_XML = 2;

    protected int kvType = KV_MEM;
    protected String fdbClusterFile;
    protected File xmlFile;
    protected byte[] keyPrefix;
    protected int schemaVersion;
    protected boolean newSchema;
    protected boolean verbose;
    protected boolean readOnly;

    protected KVDatabase kvdb;

    /**
     * @return -1 to proceed, otherwise process exit value
     */
    public int parseOptions(ArrayDeque<String> params) {
        while (!params.isEmpty() && params.peekFirst().startsWith("-")) {
            final String option = params.removeFirst();
            if (option.equals("-h") || option.equals("--help")) {
                this.usageMessage();
                return 0;
            } else if (option.equals("-ro") || option.equals("--read-only"))
                this.readOnly = true;
            else if (option.equals("-v") || option.equals("--verbose"))
                this.verbose = true;
            else if (option.equals("--version")) {
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
                    this.keyPrefix = value.getBytes(Charset.forName("UTF-8"));
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
            else if (!this.parseOption(option, params)) {
                System.err.println(this.getName() + ": unknown option `" + option + "'");
                this.usageError();
            }
        }
        if (this.kvType != KV_FDB && this.keyPrefix != null) {
            System.err.println(this.getName() + ": option `--prefix' is only valid in combination with `--fdb'");
            this.usageError();
        }
        return -1;
    }

    protected boolean parseOption(String option, ArrayDeque<String> params) {
        return false;
    }

    protected void startupKVDatabase() {
        if (this.kvdb != null)
            this.shutdownKVDatabase();
        switch (this.kvType) {
        case KV_MEM:
            this.kvdb = new SimpleKVDatabase();
            break;
        case KV_FDB:
        {
            final FoundationKVDatabase fdb = new FoundationKVDatabase();
            fdb.setClusterFilePath(this.fdbClusterFile);
            fdb.setKeyPrefix(this.keyPrefix);
            fdb.start();
            this.kvdb = fdb;
            break;
        }
        case KV_XML:
            this.kvdb = new XMLKVDatabase(this.xmlFile);
            break;
        default:
            throw new RuntimeException("internal error");
        }
    }

    protected void shutdownKVDatabase() {
        if (this.kvdb != null) {
            switch (this.kvType) {
            case KV_FDB:
                ((FoundationKVDatabase)this.kvdb).stop();
                break;
            default:
                break;
            }
            this.kvdb = null;
        }
    }

    protected abstract String getName();

    protected void outputFlags() {
        System.err.println("  --fdb file        Use FoundationDB with specified cluster file");
        System.err.println("  --mem             Use an empty in-memory database (default)");
        System.err.println("  --prefix prefix   FoundationDB key prefix (hex or string)");
        System.err.println("  --read-only       Disallow database modifications");
        System.err.println("  --new-schema      Allow recording of a new database schema version");
        System.err.println("  --xml file        Use the specified XML flat file database");
        System.err.println("  --version num     Specify database schema version (default highest recorded)");
        System.err.println("  -h, --help        Show this help message");
        System.err.println("  -v, --verbose     Show verbose error messages");
    }
}

