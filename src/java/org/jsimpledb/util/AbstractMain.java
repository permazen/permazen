
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Function;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.dellroad.stuff.main.MainClass;
import org.dellroad.stuff.net.TCPNetwork;
import org.jsimpledb.JSimpleDBFactory;
import org.jsimpledb.annotation.JFieldType;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.array.ArrayKVDatabase;
import org.jsimpledb.kv.array.AtomicArrayKVStore;
import org.jsimpledb.kv.bdb.BerkeleyKVDatabase;
import org.jsimpledb.kv.fdb.FoundationKVDatabase;
import org.jsimpledb.kv.leveldb.LevelDBAtomicKVStore;
import org.jsimpledb.kv.leveldb.LevelDBKVDatabase;
import org.jsimpledb.kv.mvcc.AtomicKVDatabase;
import org.jsimpledb.kv.mvcc.AtomicKVStore;
import org.jsimpledb.kv.raft.RaftKVDatabase;
import org.jsimpledb.kv.rocksdb.RocksDBAtomicKVStore;
import org.jsimpledb.kv.rocksdb.RocksDBKVDatabase;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.kv.simple.XMLKVDatabase;
import org.jsimpledb.kv.sql.MySQLKVDatabase;
import org.jsimpledb.spring.JSimpleDBClassScanner;
import org.jsimpledb.spring.JSimpleDBFieldTypeScanner;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Support superclass for main entry point classes.
 */
public abstract class AbstractMain extends MainClass {

    private static final File DEMO_XML_FILE = new File("demo-database.xml");
    private static final File DEMO_SUBDIR = new File("demo-classes");
    private static final String MYSQL_DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

    // DBTypes that have multiple config flags
    protected FoundationDBType foundationDBType;
    protected BerkeleyDBType berkeleyDBType;
    protected RaftDBType raftDBType;

    // Schema
    protected int schemaVersion;
    protected HashSet<Class<?>> schemaClasses;
    protected HashSet<Class<? extends FieldType<?>>> fieldTypeClasses;
    protected boolean allowNewSchema;

    protected DBType<?> dbType;
    protected KVDatabase kvdb;
    protected String databaseDescription;

    // Misc
    protected boolean verbose;
    protected boolean readOnly;
    protected boolean allowAutoDemo = true;

    /**
     * Parse command line options.
     *
     * @param params command line parameters
     * @return -1 to proceed, otherwise process exit value
     */
    public int parseOptions(ArrayDeque<String> params) {

        // Parse options
        final ArrayList<DBType<?>> dbTypes = new ArrayList<>();
        final LinkedHashSet<String> modelPackages = new LinkedHashSet<>();
        final LinkedHashSet<String> typePackages = new LinkedHashSet<>();
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
                modelPackages.add(params.removeFirst());
            } else if (option.equals("--type-pkg")) {
                if (params.isEmpty())
                    this.usageError();
                typePackages.add(params.removeFirst());
            } else if (option.equals("-p") || option.equals("--pkg")) {
                if (params.isEmpty())
                    this.usageError();
                final String packageName = params.removeFirst();
                modelPackages.add(packageName);
                typePackages.add(packageName);
            } else if (option.equals("--new-schema")) {
                this.allowNewSchema = true;
                this.allowAutoDemo = false;
            } else if (option.equals("--mem"))
                dbTypes.add(new MemoryDBType());
            else if (option.equals("--fdb-prefix")) {
                if (params.isEmpty())
                    this.usageError();
                if (this.foundationDBType == null) {
                    System.err.println(this.getName() + ": `--fdb' must appear before `" + option + "'");
                    return 1;
                }
                final String value = params.removeFirst();
                byte[] prefix;
                try {
                    prefix = ByteUtil.parse(value);
                } catch (IllegalArgumentException e) {
                    prefix = value.getBytes(Charset.forName("UTF-8"));
                }
                if (prefix.length > 0)
                    this.allowAutoDemo = false;
                this.foundationDBType.setPrefix(prefix);
            } else if (option.equals("--fdb")) {
                if (params.isEmpty())
                    this.usageError();
                final String clusterFile = params.removeFirst();
                if (!new File(clusterFile).exists()) {
                    System.err.println(this.getName() + ": file `" + clusterFile + "' does not exist");
                    return 1;
                }
                this.foundationDBType = new FoundationDBType(clusterFile);
                dbTypes.add(this.foundationDBType);
            } else if (option.equals("--xml")) {
                if (params.isEmpty())
                    this.usageError();
                dbTypes.add(new XMLDBType(new File(params.removeFirst())));
            } else if (option.equals("--bdb")) {
                if (params.isEmpty())
                    this.usageError();
                final File dir = new File(params.removeFirst());
                if (!this.createDirectory(dir))
                    return 1;
                this.berkeleyDBType = new BerkeleyDBType(dir);
                dbTypes.add(this.berkeleyDBType);
            } else if (option.equals("--bdb-database")) {
                if (params.isEmpty())
                    this.usageError();
                if (this.berkeleyDBType == null) {
                    System.err.println(this.getName() + ": `--bdb' must appear before `" + option + "'");
                    return 1;
                }
                this.berkeleyDBType.setDatabaseName(params.removeFirst());
            } else if (option.equals("--mysql")) {
                if (params.isEmpty())
                    this.usageError();
                dbTypes.add(new MySQLDBType(params.removeFirst()));
            } else if (option.equals("--leveldb")) {
                if (params.isEmpty())
                    this.usageError();
                final File dir = new File(params.removeFirst());
                if (!this.createDirectory(dir))
                    return 1;
                dbTypes.add(new LevelDBType(dir));
            } else if (option.equals("--rocksdb")) {
                if (params.isEmpty())
                    this.usageError();
                final File dir = new File(params.removeFirst());
                if (!this.createDirectory(dir))
                    return 1;
                dbTypes.add(new RocksDBType(dir));
            } else if (option.equals("--arraydb")) {
                if (params.isEmpty())
                    this.usageError();
                final File dir = new File(params.removeFirst());
                if (!this.createDirectory(dir))
                    return 1;
                dbTypes.add(new ArrayDBType(dir));
            } else if (option.equals("--raft") || option.equals("--raft-dir")) {            // --raft-dir is backward compat.
                if (params.isEmpty())
                    this.usageError();
                final File dir = new File(params.removeFirst());
                if (!this.createDirectory(dir))
                    return 1;
                this.raftDBType = new RaftDBType(dir);
                dbTypes.add(this.raftDBType);
            } else if (option.matches("--raft-((min|max)-election|heartbeat)-timeout")) {
                if (params.isEmpty())
                    this.usageError();
                if (this.raftDBType == null) {
                    System.err.println(this.getName() + ": `--raft' must appear before `" + option + "'");
                    return 1;
                }
                final String tstring = params.removeFirst();
                final int timeout;
                try {
                    timeout = Integer.parseInt(tstring);
                } catch (Exception e) {
                    System.err.println(this.getName() + ": invalid timeout value `" + tstring + "': " + e.getMessage());
                    return 1;
                }
                if (option.equals("--raft-min-election-timeout"))
                    this.raftDBType.setMinElectionTimeout(timeout);
                else if (option.equals("--raft-max-election-timeout"))
                    this.raftDBType.setMaxElectionTimeout(timeout);
                else if (option.equals("--raft-heartbeat-timeout"))
                    this.raftDBType.setHeartbeatTimeout(timeout);
                else
                    throw new RuntimeException("internal error");
            } else if (option.equals("--raft-identity")) {
                if (params.isEmpty())
                    this.usageError();
                if (this.raftDBType == null) {
                    System.err.println(this.getName() + ": `--raft' must appear before `" + option + "'");
                    return 1;
                }
                this.raftDBType.setIdentity(params.removeFirst());
            } else if (option.equals("--raft-address")) {
                if (params.isEmpty())
                    this.usageError();
                if (this.raftDBType == null) {
                    System.err.println(this.getName() + ": `--raft' must appear before `" + option + "'");
                    return 1;
                }
                final String address = params.removeFirst();
                this.raftDBType.setAddress(TCPNetwork.parseAddressPart(address));
                this.raftDBType.setPort(TCPNetwork.parsePortPart(address, this.raftDBType.getPort()));
            } else if (option.equals("--raft-port")) {
                if (params.isEmpty())
                    this.usageError();
                if (this.raftDBType == null) {
                    System.err.println(this.getName() + ": `--raft' must appear before `" + option + "'");
                    return 1;
                }
                final String portString = params.removeFirst();
                final int port = TCPNetwork.parsePortPart("x:" + portString, -1);
                if (port == -1) {
                    System.err.println(this.getName() + ": invalid TCP port `" + portString + "'");
                    return 1;
                }
                this.raftDBType.setPort(port);
            } else if (option.equals("--"))
                break;
            else if (!this.parseOption(option, params)) {
                System.err.println(this.getName() + ": unknown option `" + option + "'");
                this.usageError();
                return 1;
            }
        }

        // Additional logic post-processing of options
        if (!modelPackages.isEmpty() || !typePackages.isEmpty())
            this.allowAutoDemo = false;

        // Pull out local store k/v type for Raft
        if (this.raftDBType != null) {
            final int raftIndex = dbTypes.indexOf(this.raftDBType);
            if (raftIndex == dbTypes.size() - 1) {
                System.err.println(this.getName() + ": Raft raft requires an additional peristent store"
                  + " to be used for private local storage, specified after `--raft'; use one of `--arraydb', etc.");
                return 1;
            }
            final DBType<?> localStorageDBType = dbTypes.remove(raftIndex + 1);
            if (!localStorageDBType.canBeRaftLocalStorage()) {
                System.err.println(this.getName() + ": incompatible key/value database `" + localStorageDBType.getDescription()
                  + "' for Raft local storage");
                return 1;
            }
            this.raftDBType.setLocalStorageDBType(localStorageDBType);
        }

        // Check database choice(s)
        switch (dbTypes.size()) {
        case 0:
            if (this.allowAutoDemo && DEMO_XML_FILE.exists() && DEMO_SUBDIR.exists()) {

                // Configure database
                System.err.println(this.getName() + ": auto-configuring use of demo database `" + DEMO_XML_FILE + "'");
                if (dbTypes.isEmpty())
                    dbTypes.add(new XMLDBType(DEMO_XML_FILE));

                // Add demo subdirectory to class path
                this.appendClasspath(DEMO_SUBDIR.toString());

                // Scan classes
                this.scanModelClasses("org.jsimpledb.demo");
            } else {
                System.err.println(this.getName() + ": no key/value store specified; use one of `--arraydb', etc.");
                this.usageError();
                return 1;
            }
            break;
        case 1:
            break;
        default:
            System.err.println(this.getName() + ": more than one key/value store was specified");
            this.usageError();
            return 1;
        }
        this.dbType = dbTypes.get(0);

        // Scan for model and type classes
        final LinkedHashSet<String> emptyPackages = new LinkedHashSet<>();
        emptyPackages.addAll(modelPackages);
        emptyPackages.addAll(typePackages);
        for (String packageName : modelPackages) {
            if (this.scanModelClasses(packageName) > 0)
                emptyPackages.remove(packageName);
        }
        for (String packageName : typePackages) {
            if (this.scanTypeClasses(packageName) > 0)
                emptyPackages.remove(packageName);
        }

        // Warn if we didn't find anything
        for (String packageName : emptyPackages) {
            final boolean isModel = modelPackages.contains(packageName);
            final boolean isType = typePackages.contains(packageName);
            if (isModel && isType)
                this.log.warn("no Java model or custom FieldType classes found under package `" + packageName + "'");
            else if (isModel)
                this.log.warn("no Java model classes found under package `" + packageName + "'");
            else
                this.log.warn("no custom FieldType classes found under package `" + packageName + "'");
        }

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
     * @param option command line option (starting with `-')
     * @param params subsequent command line parameters
     * @return true if successful, false otherwise
     */
    protected boolean parseOption(String option, ArrayDeque<String> params) {
        return false;
    }

    /**
     * Append path(s) to the classpath.
     *
     * @param path classpath path component
     * @return true if successful, false if an error occured
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

    private boolean createDirectory(File dir) {
        if (!dir.exists() && !dir.mkdirs()) {
            System.err.println(this.getName() + ": could not create directory `" + dir + "'");
            return false;
        }
        if (!dir.isDirectory()) {
            System.err.println(this.getName() + ": file `" + dir + "' is not a directory");
            return false;
        }
        return true;
    }

    /**
     * Load a class.
     *
     * @param className class name
     * @return class with name {@code className}
     * @throws RuntimeException if load fails
     */
    protected Class<?> loadClass(String className) {
        try {
            return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("failed to load class `" + className + "'", e);
        }
    }

    /**
     * Start the {@link Database} based on the configured {@link KVDatabase} and {@link #fieldTypeClasses} and return it.
     *
     * @return initialized database
     */
    protected Database startupKVDatabase() {

        // Create and start up database
        this.kvdb = this.dbType.createKVDatabase();
        this.databaseDescription = this.dbType.getDescription();
        AbstractMain.startKVDatabase(this.dbType, this.kvdb);
        this.log.debug("using database: " + this.databaseDescription);

        // Construct core Database
        final Database db = new Database(this.kvdb);

        // Register custom field types
        if (this.fieldTypeClasses != null)
            db.getFieldTypeRegistry().addClasses(this.fieldTypeClasses);

        // Done
        return db;
    }

    // This method exists solely to bind the generic type parameters
    private static <T extends KVDatabase> void startKVDatabase(DBType<T> dbType, KVDatabase kvdb) {
        dbType.startKVDatabase(dbType.cast(kvdb));
    }

    /**
     * Shutdown the {@link KVDatabase}.
     */
    protected void shutdownKVDatabase() {
        AbstractMain.stopKVDatabase(this.dbType, this.kvdb);
    }

    // This method exists solely to bind the generic type parameters
    private static <T extends KVDatabase> void stopKVDatabase(DBType<T> dbType, KVDatabase kvdb) {
        dbType.stopKVDatabase(dbType.cast(kvdb));
    }

    protected abstract String getName();

    /**
     * Output usage message flag listing.
     *
     * @param subclassOpts array containing flag and description pairs
     */
    protected void outputFlags(String[][] subclassOpts) {
        final String[][] baseOpts = new String[][] {
            { "--arraydb directory",            "Use ArrayKVDatabase in specified directory" },
            { "--classpath, -cp path",          "Append to the classpath (useful with `java -jar ...')" },
            { "--fdb file",                     "Use FoundationDB with specified cluster file" },
            { "--fdb-prefix prefix",            "FoundationDB key prefix (hex or string)" },
            { "--bdb directory",                "Use Berkeley DB Java Edition in specified directory" },
            { "--bdb-database",                 "Specify Berkeley DB database name (default `"
                                                  + BerkeleyKVDatabase.DEFAULT_DATABASE_NAME + "')" },
            { "--leveldb directory",            "Use LevelDB in specified directory" },
            { "--mem",                          "Use an empty in-memory database (default)" },
            { "--mysql URL",                    "Use MySQL with the given JDBC URL" },
            { "--raft directory",               "Use Raft in specified directory" },
            { "--raft-min-election-timeout",    "Raft minimum election timeout in ms (default "
                                                  + RaftKVDatabase.DEFAULT_MIN_ELECTION_TIMEOUT + ")" },
            { "--raft-max-election-timeout",    "Raft maximum election timeout in ms (default "
                                                  + RaftKVDatabase.DEFAULT_MAX_ELECTION_TIMEOUT + ")" },
            { "--raft-heartbeat-timeout",       "Raft leader heartbeat timeout in ms (default "
                                                  + RaftKVDatabase.DEFAULT_HEARTBEAT_TIMEOUT + ")" },
            { "--raft-identity",                "Raft identity" },
            { "--raft-address address",         "Specify local Raft node's IP address" },
            { "--raft-port",                    "Specify local Raft node's TCP port (default "
                                                  + RaftKVDatabase.DEFAULT_TCP_PORT + ")" },
            { "--read-only, -ro",               "Disallow database modifications" },
            { "--rocksdb directory",            "Use RocksDB in specified directory" },
            { "--new-schema",                   "Allow recording of a new database schema version" },
            { "--xml file",                     "Use the specified XML flat file database" },
            { "--schema-version, -v num",       "Specify database schema version (default highest recorded)" },
            { "--model-pkg package",            "Scan for @JSimpleClass model classes under Java package (=> JSimpleDB mode)" },
            { "--type-pkg package",             "Scan for @JFieldType types under Java package to register custom types" },
            { "--pkg, -p package",              "Equivalent to `--model-pkg package --type-pkg package'" },
            { "--help, -h",                     "Show this help message" },
            { "--verbose",                      "Show verbose error messages" },
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

// DBType

    protected abstract class DBType<T extends KVDatabase> {

        private final Class<T> type;

        protected DBType(Class<T> type) {
            this.type = type;
        }

        public T cast(KVDatabase db) {
            return this.type.cast(db);
        }

        public AtomicKVStore createAtomicKVStore() {
            return new AtomicKVDatabase(this.createKVDatabase());
        }

        public abstract T createKVDatabase();

        public void startKVDatabase(T db) {
            db.start();
        }

        public void stopKVDatabase(T db) {
            db.stop();
        }

        public boolean canBeRaftLocalStorage() {
            return true;
        }

        public abstract String getDescription();
    }

    protected final class MemoryDBType extends DBType<SimpleKVDatabase> {

        protected MemoryDBType() {
            super(SimpleKVDatabase.class);
        }

        @Override
        public SimpleKVDatabase createKVDatabase() {
            return new SimpleKVDatabase();
        }

        @Override
        public String getDescription() {
            return "In-Memory Database";
        }
    }

    protected final class FoundationDBType extends DBType<FoundationKVDatabase> {

        private final String clusterFile;
        private byte[] prefix;

        protected FoundationDBType(String clusterFile) {
            super(FoundationKVDatabase.class);
            this.clusterFile = clusterFile;
        }

        public void setPrefix(byte[] prefix) {
            this.prefix = prefix;
        }

        @Override
        public FoundationKVDatabase createKVDatabase() {
            final FoundationKVDatabase fdb = new FoundationKVDatabase();
            fdb.setClusterFilePath(this.clusterFile);
            fdb.setKeyPrefix(prefix);
            return fdb;
        }

        @Override
        public String getDescription() {
            String desc = "FoundationDB " + new File(this.clusterFile).getName();
            if (this.prefix != null)
                desc += " [0x" + ByteUtil.toString(this.prefix) + "]";
            return desc;
        }
    }

    protected final class BerkeleyDBType extends DBType<BerkeleyKVDatabase> {

        private final File dir;
        private String databaseName = BerkeleyKVDatabase.DEFAULT_DATABASE_NAME;

        protected BerkeleyDBType(File dir) {
            super(BerkeleyKVDatabase.class);
            this.dir = dir;
        }

        public void setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
        }

        @Override
        public BerkeleyKVDatabase createKVDatabase() {
            final BerkeleyKVDatabase bdb = new BerkeleyKVDatabase();
            bdb.setDirectory(this.dir);
            bdb.setDatabaseName(this.databaseName);
            return bdb;
        }

        @Override
        public String getDescription() {
            return "BerkeleyDB " + this.dir.getName();
        }
    }

    protected final class XMLDBType extends DBType<XMLKVDatabase> {

        private final File xmlFile;

        protected XMLDBType(File xmlFile) {
            super(XMLKVDatabase.class);
            this.xmlFile = xmlFile;
        }

        @Override
        public XMLKVDatabase createKVDatabase() {
            return new XMLKVDatabase(this.xmlFile);
        }

        @Override
        public String getDescription() {
            return "XML DB " + this.xmlFile.getName();
        }
    }

    protected final class LevelDBType extends DBType<LevelDBKVDatabase> {

        private final File dir;

        protected LevelDBType(File dir) {
            super(LevelDBKVDatabase.class);
            this.dir = dir;
        }

        @Override
        public LevelDBKVDatabase createKVDatabase() {
            final LevelDBKVDatabase leveldb = new LevelDBKVDatabase();
            leveldb.setKVStore(this.createAtomicKVStore());
            return leveldb;
        }

        @Override
        public LevelDBAtomicKVStore createAtomicKVStore() {
            final LevelDBAtomicKVStore kvstore = new LevelDBAtomicKVStore();
            kvstore.setDirectory(this.dir);
            kvstore.setCreateIfMissing(true);
            return kvstore;
        }

        @Override
        public String getDescription() {
            return "LevelDB " + this.dir.getName();
        }
    }

    protected final class RocksDBType extends DBType<RocksDBKVDatabase> {

        private final File dir;

        protected RocksDBType(File dir) {
            super(RocksDBKVDatabase.class);
            this.dir = dir;
        }

        @Override
        public RocksDBKVDatabase createKVDatabase() {
            final RocksDBKVDatabase rocksdb = new RocksDBKVDatabase();
            rocksdb.setKVStore(this.createAtomicKVStore());
            return rocksdb;
        }

        @Override
        public RocksDBAtomicKVStore createAtomicKVStore() {
            final RocksDBAtomicKVStore kvstore = new RocksDBAtomicKVStore();
            kvstore.setDirectory(this.dir);
            return kvstore;
        }

        @Override
        public String getDescription() {
            return "RocksDB " + this.dir.getName();
        }
    }

    protected final class ArrayDBType extends DBType<ArrayKVDatabase> {

        private final File dir;

        protected ArrayDBType(File dir) {
            super(ArrayKVDatabase.class);
            this.dir = dir;
        }

        @Override
        public ArrayKVDatabase createKVDatabase() {
            final ArrayKVDatabase arraydb = new ArrayKVDatabase();
            arraydb.setKVStore(this.createAtomicKVStore());
            return arraydb;
        }

        @Override
        public AtomicArrayKVStore createAtomicKVStore() {
            final AtomicArrayKVStore kvstore = new AtomicArrayKVStore();
            kvstore.setDirectory(this.dir);
            return kvstore;
        }

        @Override
        public String getDescription() {
            return "ArrayDB " + this.dir.getName();
        }
    }

    protected final class MySQLDBType extends DBType<MySQLKVDatabase> {

        private final String jdbcUrl;

        protected MySQLDBType(String jdbcUrl) {
            super(MySQLKVDatabase.class);
            this.jdbcUrl = jdbcUrl;
        }

        @Override
        public MySQLKVDatabase createKVDatabase() {
            try {
                Class.forName(MYSQL_DRIVER_CLASS_NAME);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("can't load MySQL driver class `" + MYSQL_DRIVER_CLASS_NAME + "'", e);
            }
            final MySQLKVDatabase mysql = new MySQLKVDatabase();
            mysql.setDataSource(new DriverManagerDataSource(this.jdbcUrl));
            return mysql;
        }

        @Override
        public String getDescription() {
            return "MySQL";
        }
    }

    protected final class RaftDBType extends DBType<RaftKVDatabase> {

        private final File directory;
        private final RaftKVDatabase raft = new RaftKVDatabase();

        private DBType<?> localStorageDBType;
        private String address;
        private int port = RaftKVDatabase.DEFAULT_TCP_PORT;

        protected RaftDBType(File directory) {
            super(RaftKVDatabase.class);
            this.directory = directory;
            this.raft.setLogDirectory(this.directory);
        }

        @Override
        public RaftKVDatabase createKVDatabase() {

            // Set up Raft local storage
            this.raft.setKVStore(this.localStorageDBType.createAtomicKVStore());

            // Setup network
            final TCPNetwork network = new TCPNetwork(RaftKVDatabase.DEFAULT_TCP_PORT);
            try {
                network.setListenAddress(this.address != null ?
                  new InetSocketAddress(InetAddress.getByName(this.address), this.port) : new InetSocketAddress(this.port));
            } catch (UnknownHostException e) {
                throw new RuntimeException("can't resolve address `" + this.address + "'", e);
            }
            this.raft.setNetwork(network);

            // Done
            return this.raft;
        }

        public void setLocalStorageDBType(DBType<?> localStorageDBType) {
            this.localStorageDBType = localStorageDBType;
        }

        public void setIdentity(String identity) {
            this.raft.setIdentity(identity);
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public int getPort() {
            return this.port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public void setMinElectionTimeout(int minElectionTimeout) {
            this.raft.setMinElectionTimeout(minElectionTimeout);
        }

        public void setMaxElectionTimeout(int maxElectionTimeout) {
            this.raft.setMaxElectionTimeout(maxElectionTimeout);
        }

        public void setHeartbeatTimeout(int heartbeatTimeout) {
            this.raft.setHeartbeatTimeout(heartbeatTimeout);
        }

        @Override
        public boolean canBeRaftLocalStorage() {
            return false;
        }

        @Override
        public String getDescription() {
            return "Raft " + (this.directory != null ? this.directory.getName() : "?");
        }
    }
}

