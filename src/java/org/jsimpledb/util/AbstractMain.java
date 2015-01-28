
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import com.google.common.base.Function;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;

import org.dellroad.stuff.main.MainClass;
import org.jsimpledb.JSimpleDBFactory;
import org.jsimpledb.annotation.JFieldType;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.bdb.BerkeleyKVDatabase;
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
    protected static final int KV_BDB = 3;

    private static final File DEMO_XML_FILE = new File("demo-database.xml");
    private static final File DEMO_SUBDIR = new File("demo-classes");

    protected int kvType = KV_MEM;
    protected String fdbClusterFile;
    protected File bdbDirectory;
    protected String bdbDatabaseName = BerkeleyKVDatabase.DEFAULT_DATABASE_NAME;
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
    protected boolean allowAutoDemo = true;

    /**
     * Parse command line options.
     *
     * @return -1 to proceed, otherwise process exit value
     */
    public int parseOptions(ArrayDeque<String> params) {

        // Parse options
        while (!params.isEmpty() && params.peekFirst().startsWith("-")) {
            final String option = params.removeFirst();
            if (option.equals("-h") || option.equals("--help")) {
                this.usageMessage();
                return 0;
            } else if (option.equals("-ro") || option.equals("--read-only"))
                this.readOnly = true;
            else if (option.equals("-cp") || option.equals("--classpath")) {
                if (params.isEmpty())
                    this.usageError();
                if (!this.appendClasspath(params.removeFirst()))
                    return 1;
            } else if (option.equals("--verbose"))
                this.verbose = true;
            else if (option.equals("-v") || option.equals("--schema-version")) {
                if (params.isEmpty())
                    this.usageError();
                final String vstring = params.removeFirst();
                try {
                    this.schemaVersion = Integer.parseInt(vstring);
                    if (this.schemaVersion < 0)
                        throw new IllegalArgumentException("schema version is negative");
                } catch (Exception e) {
                    System.err.println(this.getName() + ": invalid schema version `" + vstring + "': " + e.getMessage());
                    return 1;
                }
            } else if (option.equals("--model-pkg")) {
                if (params.isEmpty())
                    this.usageError();
                final String packageName = params.removeFirst();
                if (this.scanModelClasses(packageName) == 0)
                    this.log.warn("no Java model classes found under package `" + packageName + "'");
                this.allowAutoDemo = false;
            } else if (option.equals("--type-pkg")) {
                if (params.isEmpty())
                    this.usageError();
                final String packageName = params.removeFirst();
                if (this.scanTypeClasses(packageName) == 0)
                    this.log.warn("no custom FieldType classes found under package `" + packageName + "'");
            } else if (option.equals("-p") || option.equals("--pkg")) {
                if (params.isEmpty())
                    this.usageError();
                final String packageName = params.removeFirst();
                final int numModelClasses = this.scanModelClasses(packageName);
                final int numTypeClasses = this.scanTypeClasses(packageName);
                if (numModelClasses + numTypeClasses == 0)
                    this.log.warn("no Java model or custom FieldType classes found under package `" + packageName + "'");
                this.allowAutoDemo = false;
            } else if (option.equals("--new-schema")) {
                this.allowNewSchema = true;
                this.allowAutoDemo = false;
            } else if (option.equals("--mem")) {
                this.kvType = KV_MEM;
                this.allowAutoDemo = false;
            } else if (option.equals("--prefix")) {
                if (params.isEmpty())
                    this.usageError();
                final String value = params.removeFirst();
                try {
                    this.keyPrefix = ByteUtil.parse(value);
                } catch (IllegalArgumentException e) {
                    this.keyPrefix = value.getBytes(Charset.forName("UTF-8"));
                }
                if (this.keyPrefix.length > 0)
                    this.allowAutoDemo = false;
            } else if (option.equals("--fdb")) {
                if (params.isEmpty())
                    this.usageError();
                this.kvType = KV_FDB;
                this.fdbClusterFile = params.removeFirst();
                if (!new File(this.fdbClusterFile).exists()) {
                    System.err.println(this.getName() + ": file `" + this.fdbClusterFile + "' does not exist");
                    return 1;
                }
                this.allowAutoDemo = false;
            } else if (option.equals("--xml")) {
                if (params.isEmpty())
                    this.usageError();
                this.kvType = KV_XML;
                this.xmlFile = new File(params.removeFirst());
                this.allowAutoDemo = false;
            } else if (option.equals("--bdb")) {
                if (params.isEmpty())
                    this.usageError();
                this.kvType = KV_BDB;
                this.bdbDirectory = new File(params.removeFirst());
                if (!this.bdbDirectory.exists()) {
                    System.err.println(this.getName() + ": directory `" + this.bdbDirectory + "' does not exist");
                    return 1;
                }
                if (!this.bdbDirectory.isDirectory()) {
                    System.err.println(this.getName() + ": file `" + this.bdbDirectory + "' is not a directory");
                    return 1;
                }
                this.allowAutoDemo = false;
            } else if (option.equals("--bdb-database")) {
                if (params.isEmpty())
                    this.usageError();
                this.bdbDatabaseName = params.removeFirst();
            } else if (option.equals("--"))
                break;
            else if (!this.parseOption(option, params)) {
                System.err.println(this.getName() + ": unknown option `" + option + "'");
                this.usageError();
                return 1;
            }
        }
        if (this.kvType != KV_FDB && this.keyPrefix != null) {
            System.err.println(this.getName() + ": option `--prefix' is only valid in combination with `--fdb'");
            this.usageError();
            return 1;
        }

        // Automatically go into demo mode if appropriate
        if (this.allowAutoDemo && DEMO_XML_FILE.exists() && DEMO_SUBDIR.exists())
            this.configureDemoMode();

        // Done
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

    public JSimpleDBFactory getJSimpleDBFactory(Database db) {
        return new JSimpleDBFactory()
          .setModelClasses(this.schemaClasses)
          .setSchemaVersion(this.schemaVersion)
          .setDatabase(db);
    }

    /**
     * Subclass hook to parse unrecognized command line options.
     *
     * @return true if successful, false otherwise
     */
    protected boolean parseOption(String option, ArrayDeque<String> params) {
        return false;
    }

    protected void configureDemoMode() {

        // Configure database
        System.err.println(this.getName() + ": auto-configuring use of demo database `" + DEMO_XML_FILE + "'");
        this.kvType = KV_XML;
        this.xmlFile = DEMO_XML_FILE;

        // Add demo subdirectory to class path
        this.appendClasspath(DEMO_SUBDIR.toString());

        // Scan classes
        this.scanModelClasses("org.jsimpledb.demo");
    }

    /**
     * Append path(s) to the classpath.
     */
    protected boolean appendClasspath(String path) {
        this.log.trace("adding classpath `" + path + "' to system classpath");
        try {

            // Get URLClassLoader.addURL() method and make accessible
            final Method addURLMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            addURLMethod.setAccessible(true);

            // Split path and add components
            for (String file : path.split(System.getProperty("path.separator", ":"))) {
                if (file.length() == 0)
                    continue;
                addURLMethod.invoke(ClassLoader.getSystemClassLoader(), new Object[] { new File(file).toURI().toURL() });
                this.log.trace("added path component `" + file + "' to system classpath");
            }
            return true;
        } catch (Exception e) {
            this.log.error("can't append `" + path + " to classpath: " + e, e);
            return false;
        }
    }

    private int scanModelClasses(String pkgname) {
        if (this.schemaClasses == null)
            this.schemaClasses = new HashSet<>();
        int count = 0;
        for (String className : new JSimpleDBClassScanner().scanForClasses(pkgname.split("[\\s,]"))) {
            this.log.debug("loading Java model class " + className);
            this.schemaClasses.add(this.loadClass(className));
            count++;
        }
        return count;
    }

    private int scanTypeClasses(String pkgname) {

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
        int count = 0;
        for (String className : new JSimpleDBFieldTypeScanner().scanForClasses(pkgname.split("[\\s,]"))) {
            this.log.debug("loading custom FieldType class " + className);
            this.fieldTypeClasses.add(checkFunction.apply(this.loadClass(className)));
            count++;
        }
        return count;
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
            this.databaseDescription = "FoundationDB " + new File(this.fdbClusterFile).getName();
            if (this.keyPrefix != null)
                this.databaseDescription += " [0x" + ByteUtil.toString(this.keyPrefix) + "]";
            break;
        }
        case KV_BDB:
        {
            final BerkeleyKVDatabase bdb = new BerkeleyKVDatabase();
            bdb.setDirectory(this.bdbDirectory);
            bdb.setDatabaseName(this.bdbDatabaseName);
//            if (this.readOnly)
//                bdb.setDatabaseConfig(bdb.getDatabaseConfig().setReadOnly(true));
            bdb.start();
            this.kvdb = bdb;
            this.databaseDescription = "BerkeleyDB " + this.bdbDirectory.getName();
            break;
        }
        case KV_XML:
            this.kvdb = new XMLKVDatabase(this.xmlFile);
            this.databaseDescription = "XML DB " + this.xmlFile.getName();
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
            case KV_BDB:
                ((BerkeleyKVDatabase)this.kvdb).stop();
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
            { "--classpath, -cp path",      "Append to the classpath (useful with `java -jar ...')" },
            { "--fdb file",                 "Use FoundationDB with specified cluster file" },
            { "--bdb directory",            "Use Berkeley DB Java Edition in specified directory" },
            { "--bdb-database",             "Specify Berkeley DB database name (default `"
                                              + BerkeleyKVDatabase.DEFAULT_DATABASE_NAME + "')" },
            { "--mem",                      "Use an empty in-memory database (default)" },
            { "--prefix prefix",            "FoundationDB key prefix (hex or string)" },
            { "--read-only, -ro",           "Disallow database modifications" },
            { "--new-schema",               "Allow recording of a new database schema version" },
            { "--xml file",                 "Use the specified XML flat file database" },
            { "--schema-version, -v num",   "Specify database schema version (default highest recorded)" },
            { "--model-pkg package",        "Scan for @JSimpleClass model classes under Java package (=> JSimpleDB mode)" },
            { "--type-pkg package",         "Scan for @JFieldType types under Java package to register custom types" },
            { "--pkg, -p package",          "Equivalent to `--model-pkg package --type-pkg package'" },
            { "--help, -h",                 "Show this help message" },
            { "--verbose",                  "Show verbose error messages" },
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

