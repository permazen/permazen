
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import com.google.common.base.Function;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;

import org.dellroad.stuff.main.MainClass;
import org.jsimpledb.annotation.JFieldType;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.fdb.FoundationKVDatabase;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.kv.simple.XMLKVDatabase;
import org.jsimpledb.spring.JSimpleDBClassScanner;
import org.jsimpledb.spring.JSimpleDBFieldTypeScanner;

/**
 * Support superclass for main entry point classes.
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
    protected HashSet<Class<?>> schemaClasses;
    protected HashSet<Class<? extends FieldType<?>>> fieldTypeClasses;
    protected boolean allowNewSchema;
    protected boolean verbose;
    protected boolean readOnly;

    protected KVDatabase kvdb;
    protected String databaseDescription;

    /**
     * Parse command line options.
     *
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
            } else if (option.equals("--schema-pkg")) {
                if (params.isEmpty())
                    this.usageError();
                this.scanSchemaClasses(params.removeFirst());
            } else if (option.equals("--types-pkg")) {
                if (params.isEmpty())
                    this.usageError();
                this.scanTypeClasses(params.removeFirst());
            } else if (option.equals("--scan-pkg")) {
                if (params.isEmpty())
                    this.usageError();
                this.scanSchemaClasses(params.removeFirst());
                this.scanTypeClasses(params.removeFirst());
            } else if (option.equals("--new-schema"))
                this.allowNewSchema = true;
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

    public int getSchemaVersion() {
        return this.schemaVersion;
    }

    public boolean isAllowNewSchema() {
        return this.allowNewSchema;
    }

    public boolean isReadOnly() {
        return this.readOnly;
    }

    public String getDatabaseDescription() {
        return this.databaseDescription;
    }

    /**
     * Subclass hook to parse unrecognized command line options.
     *
     * @return true if successful, false otherwise
     */
    protected boolean parseOption(String option, ArrayDeque<String> params) {
        return false;
    }

    private void scanSchemaClasses(String pkgname) {
        if (this.schemaClasses == null)
            this.schemaClasses = new HashSet<>();
        for (String className : new JSimpleDBClassScanner().scanForClasses(pkgname.split("[\\s,]")))
            this.schemaClasses.add(this.loadClass(className));
    }

    private void scanTypeClasses(String pkgname) {

        // Check types of annotated classes as we scan them
        final Function<Class<?>, Class<? extends FieldType<?>>> checkFunction
          = new Function<Class<?>, Class<? extends FieldType<?>>>() {
            @Override
            @SuppressWarnings("unchecked")
            public Class<? extends FieldType<?>> apply(Class<?> type) {
                try {
                    return (Class<? extends FieldType<?>>)type.asSubclass(FieldType.class);
                } catch (ClassCastException e) {
                    throw new IllegalArgumentException("invalid @" + JFieldType.class.getSimpleName() + " annotation on "
                      + type + ": type is not a subclass of " + FieldType.class);
                }
            }
        };

        // Scan classes
        if (this.fieldTypeClasses == null)
            this.fieldTypeClasses = new HashSet<>();
        for (String className : new JSimpleDBFieldTypeScanner().scanForClasses(pkgname.split("[\\s,]")))
            this.fieldTypeClasses.add(checkFunction.apply(this.loadClass(className)));
    }

    /**
     * Load a class.
     *
     * @throws RuntimeException if load fails
     */
    protected Class<?> loadClass(String className) {
        try {
            return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("failed to load class `" + className + "'", e);
        }
    }

    protected void startupKVDatabase() {
        if (this.kvdb != null)
            this.shutdownKVDatabase();
        switch (this.kvType) {
        case KV_MEM:
            this.kvdb = new SimpleKVDatabase();
            this.databaseDescription = "In-Memory Database";
            break;
        case KV_FDB:
        {
            final FoundationKVDatabase fdb = new FoundationKVDatabase();
            fdb.setClusterFilePath(this.fdbClusterFile);
            fdb.setKeyPrefix(this.keyPrefix);
            fdb.start();
            this.kvdb = fdb;
            this.databaseDescription = "FoundationDB";
            if (this.keyPrefix != null)
                this.databaseDescription += " [0x" + ByteUtil.toString(this.keyPrefix) + "]";
            break;
        }
        case KV_XML:
            this.kvdb = new XMLKVDatabase(this.xmlFile);
            this.databaseDescription = this.xmlFile.getName();
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

    /**
     * Output usage message flag listing.
     */
    protected void outputFlags(String[][] subclassOpts) {
        final String[][] baseOpts = new String[][] {
            { "--fdb file",             "Use FoundationDB with specified cluster file" },
            { "--mem",                  "Use an empty in-memory database (default)" },
            { "--prefix prefix",        "FoundationDB key prefix (hex or string)" },
            { "--read-only",            "Disallow database modifications" },
            { "--new-schema",           "Allow recording of a new database schema version" },
            { "--xml file",             "Use the specified XML flat file database" },
            { "--version num",          "Specify database schema version (default highest recorded)" },
            { "--schema-pkg package",   "Scan for @JSimpleClass types under Java package to build schema (=> JSimpleDB mode)" },
            { "--types-pkg package",    "Scan for @JFieldType types under Java package to register custom types" },
            { "--scan-pkg package",     "Equivalent to `--schema-pkg package --types-pkg package'" },
            { "--help, -h",             "Show this help message" },
            { "--verbose, -v",          "Show verbose error messages" },
        };
        final String[][] combinedOpts = new String[baseOpts.length + subclassOpts.length][];
        System.arraycopy(baseOpts, 0, combinedOpts, 0, baseOpts.length);
        System.arraycopy(subclassOpts, 0, combinedOpts, baseOpts.length, subclassOpts.length);
        Arrays.sort(combinedOpts, new Comparator<String[]>() {
            @Override
            public int compare(String[] opt1, String[] opt2) {
                return opt1[0].compareTo(opt2[0]);
            }
        });
        int width = 0;
        for (String[] opt : combinedOpts)
            width = Math.max(width, opt[0].length());
        for (String[] opt : combinedOpts)
            System.err.println(String.format("  %-" + width + "s  %s", opt[0], opt[1]));
    }
}

