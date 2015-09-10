
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.dellroad.stuff.main.MainClass;
import org.dellroad.stuff.net.TCPNetwork;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JSimpleDBFactory;
import org.jsimpledb.ValidationMode;
import org.jsimpledb.annotation.JFieldType;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.Transaction;
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
import org.jsimpledb.schema.SchemaModel;
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

    // These are like an Enum<DBType>
    protected final MemoryDBType memoryDBType = new MemoryDBType();
    protected final FoundationDBType foundationDBType = new FoundationDBType();
    protected final BerkeleyDBType berkeleyDBType = new BerkeleyDBType();
    protected final XMLDBType xmlDBType = new XMLDBType();
    protected final LevelDBType levelDBType = new LevelDBType();
    protected final RocksDBType rocksDBType = new RocksDBType();
    protected final ArrayDBType arrayDBType = new ArrayDBType();
    protected final MySQLDBType mySQLType = new MySQLDBType();
    protected final RaftDBType raftDBType = new RaftDBType();

    // FDB config
    protected String fdbClusterFile;
    protected byte[] fdbKeyPrefix;

    // BDB config
    protected File bdbDirectory;
    protected String bdbDatabaseName = BerkeleyKVDatabase.DEFAULT_DATABASE_NAME;

    // LevelDB config
    protected File leveldbDirectory;

    // RocksDB config
    protected File rocksdbDirectory;

    // ArrayKVDatabase config
    protected File arraydbDirectory;

    // Raft config
    protected AtomicKVStore raftKVStore;
    protected File raftDirectory;
    protected String raftIdentity;
    protected String raftAddress;
    protected int raftPort = RaftKVDatabase.DEFAULT_TCP_PORT;
    protected int raftMinElectionTimeout = -1;
    protected int raftMaxElectionTimeout = -1;
    protected int raftHeartbeatTimeout = -1;

    // XML config
    protected File xmlFile;

    // MySQL config
    protected String jdbcUrl;

    // Schema
    protected int schemaVersion;
    protected HashSet<Class<?>> schemaClasses;
    protected HashSet<Class<? extends FieldType<?>>> fieldTypeClasses;
    protected boolean allowNewSchema;

    protected HashSet<DBType<?>> dbTypes = new HashSet<>();
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
                this.dbTypes.add(this.memoryDBType);
            else if (option.equals("--fdb-prefix")) {
                if (params.isEmpty())
                    this.usageError();
                final String value = params.removeFirst();
                try {
                    this.fdbKeyPrefix = ByteUtil.parse(value);
                } catch (IllegalArgumentException e) {
                    this.fdbKeyPrefix = value.getBytes(Charset.forName("UTF-8"));
                }
                if (this.fdbKeyPrefix.length > 0)
                    this.allowAutoDemo = false;
            } else if (option.equals("--fdb")) {
                if (params.isEmpty())
                    this.usageError();
                this.dbTypes.add(this.foundationDBType);
                this.fdbClusterFile = params.removeFirst();
                if (!new File(this.fdbClusterFile).exists()) {
                    System.err.println(this.getName() + ": file `" + this.fdbClusterFile + "' does not exist");
                    return 1;
                }
            } else if (option.equals("--xml")) {
                if (params.isEmpty())
                    this.usageError();
                this.dbTypes.add(this.xmlDBType);
                this.xmlFile = new File(params.removeFirst());
            } else if (option.equals("--bdb")) {
                if (params.isEmpty())
                    this.usageError();
                this.dbTypes.add(this.berkeleyDBType);
                if (!this.createDirectory(this.bdbDirectory = new File(params.removeFirst())))
                    return 1;
            } else if (option.equals("--bdb-database")) {
                if (params.isEmpty())
                    this.usageError();
                this.bdbDatabaseName = params.removeFirst();
            } else if (option.equals("--mysql")) {
                if (params.isEmpty())
                    this.usageError();
                this.jdbcUrl = params.removeFirst();
                this.dbTypes.add(this.mySQLType);
            } else if (option.equals("--leveldb")) {
                if (params.isEmpty())
                    this.usageError();
                this.dbTypes.add(this.levelDBType);
                if (!this.createDirectory(this.leveldbDirectory = new File(params.removeFirst())))
                    return 1;
            } else if (option.equals("--rocksdb")) {
                if (params.isEmpty())
                    this.usageError();
                this.dbTypes.add(this.rocksDBType);
                if (!this.createDirectory(this.rocksdbDirectory = new File(params.removeFirst())))
                    return 1;
            } else if (option.equals("--arraydb")) {
                if (params.isEmpty())
                    this.usageError();
                this.dbTypes.add(this.arrayDBType);
                if (!this.createDirectory(this.arraydbDirectory = new File(params.removeFirst())))
                    return 1;
            } else if (option.equals("--raft-dir")) {
                if (params.isEmpty())
                    this.usageError();
                this.dbTypes.add(this.raftDBType);
                if (!this.createDirectory(this.raftDirectory = new File(params.removeFirst())))
                    return 1;
            } else if (option.matches("--raft-((min|max)-election|heartbeat)-timeout")) {
                if (params.isEmpty())
                    this.usageError();
                final String tstring = params.removeFirst();
                final int timeout;
                try {
                    timeout = Integer.parseInt(tstring);
                } catch (Exception e) {
                    System.err.println(this.getName() + ": timeout value `" + tstring + "': " + e.getMessage());
                    return 1;
                }
                if (option.equals("--raft-min-election-timeout"))
                    this.raftMinElectionTimeout = timeout;
                else if (option.equals("--raft-max-election-timeout"))
                    this.raftMaxElectionTimeout = timeout;
                else if (option.equals("--raft-heartbeat-timeout"))
                    this.raftHeartbeatTimeout = timeout;
                else
                    throw new RuntimeException("internal error");
            } else if (option.equals("--raft-identity")) {
                if (params.isEmpty())
                    this.usageError();
                this.dbTypes.add(this.raftDBType);
                this.raftIdentity = params.removeFirst();
            } else if (option.equals("--raft-address")) {
                if (params.isEmpty())
                    this.usageError();
                final String address = params.removeFirst();
                this.raftAddress = TCPNetwork.parseAddressPart(address);
                this.raftPort = TCPNetwork.parsePortPart(address, this.raftPort);
            } else if (option.equals("--raft-port")) {
                if (params.isEmpty())
                    this.usageError();
                final String portString = params.removeFirst();
                if ((this.raftPort = TCPNetwork.parsePortPart("x:" + portString, -1)) == -1) {
                    System.err.println(this.getName() + ": invalid TCP port `" + portString + "'");
                    return 1;
                }
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

        // Check database choice(s)
        switch (this.dbTypes.size()) {
        case 0:
            if (this.allowAutoDemo && DEMO_XML_FILE.exists() && DEMO_SUBDIR.exists())
                this.configureDemoMode();
            else {
                System.err.println(this.getName() + ": no key/value store specified; use one of `--mysql', etc.");
                this.usageError();
                return 1;
            }
            break;
        case 1:
            if (this.dbTypes.contains(this.raftDBType)) {
                System.err.println(this.getName() + ": Raft requires a local peristent store; use one of `--mysql', etc.");
                this.usageError();
                return 1;
            }
            break;
        default:
            if (this.dbTypes.size() > 2 || !this.dbTypes.contains(this.raftDBType)) {
                System.err.println(this.getName() + ": multiple key/value stores configured; choose only one");
                this.usageError();
                return 1;
            }
            break;
        }

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

    protected void configureDemoMode() {

        // Configure database
        System.err.println(this.getName() + ": auto-configuring use of demo database `" + DEMO_XML_FILE + "'");
        this.dbTypes.add(this.xmlDBType);
        this.xmlFile = DEMO_XML_FILE;

        // Add demo subdirectory to class path
        this.appendClasspath(DEMO_SUBDIR.toString());

        // Scan classes
        this.scanModelClasses("org.jsimpledb.demo");
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

        // Raft requires a separate AtomicKVStore to be configured first
        final boolean raft = this.dbTypes.remove(this.raftDBType);
        this.dbType = this.dbTypes.iterator().next();
        if (raft) {
            this.raftKVStore = dbType.createAtomicKVStore();
            this.dbType = this.raftDBType;
        }

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
     * Perform a test transaction.
     *
     * @param db database
     * @param schemaModel schema model
     */
    protected void performTestTransaction(Database db, SchemaModel schemaModel) {
        final Transaction tx;
        try {
            db.createTransaction(schemaModel, this.schemaVersion, this.allowNewSchema).commit();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("unable to create transaction: " + (e.getMessage() != null ? e.getMessage() : e), e);
        }
    }

    /**
     * Perform a test transaction.
     *
     * @param jdb database
     */
    protected void performTestTransaction(JSimpleDB jdb) {
        try {
            jdb.createTransaction(this.allowNewSchema, ValidationMode.AUTOMATIC).commit();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("unable to create transaction: " + (e.getMessage() != null ? e.getMessage() : e), e);
        }
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
            { "--raft-dir directory",           "Raft local persistence directory" },
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

        public abstract String getDescription();
    }

    protected final class MemoryDBType extends DBType<SimpleKVDatabase> {

        private MemoryDBType() {
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

        private FoundationDBType() {
            super(FoundationKVDatabase.class);
        }

        @Override
        public FoundationKVDatabase createKVDatabase() {
            final FoundationKVDatabase fdb = new FoundationKVDatabase();
            fdb.setClusterFilePath(AbstractMain.this.fdbClusterFile);
            fdb.setKeyPrefix(AbstractMain.this.fdbKeyPrefix);
            return fdb;
        }

        @Override
        public String getDescription() {
            String desc = "FoundationDB " + new File(AbstractMain.this.fdbClusterFile).getName();
            if (AbstractMain.this.fdbKeyPrefix != null)
                desc += " [0x" + ByteUtil.toString(AbstractMain.this.fdbKeyPrefix) + "]";
            return desc;
        }
    }

    protected final class BerkeleyDBType extends DBType<BerkeleyKVDatabase> {

        private BerkeleyDBType() {
            super(BerkeleyKVDatabase.class);
        }

        @Override
        public BerkeleyKVDatabase createKVDatabase() {
            final BerkeleyKVDatabase bdb = new BerkeleyKVDatabase();
            bdb.setDirectory(AbstractMain.this.bdbDirectory);
            bdb.setDatabaseName(AbstractMain.this.bdbDatabaseName);
//            if (AbstractMain.this.readOnly)
//                bdb.setDatabaseConfig(bdb.getDatabaseConfig().setReadOnly(true));
            return bdb;
        }

        @Override
        public String getDescription() {
            return "BerkeleyDB " + AbstractMain.this.bdbDirectory.getName();
        }
    }

    protected final class XMLDBType extends DBType<XMLKVDatabase> {

        private XMLDBType() {
            super(XMLKVDatabase.class);
        }

        @Override
        public XMLKVDatabase createKVDatabase() {
            return new XMLKVDatabase(AbstractMain.this.xmlFile);
        }

        @Override
        public String getDescription() {
            return "XML DB " + AbstractMain.this.xmlFile.getName();
        }
    }

    protected final class LevelDBType extends DBType<LevelDBKVDatabase> {

        private LevelDBType() {
            super(LevelDBKVDatabase.class);
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
            kvstore.setDirectory(AbstractMain.this.leveldbDirectory);
            kvstore.setCreateIfMissing(true);
            return kvstore;
        }

        @Override
        public String getDescription() {
            return "LevelDB " + AbstractMain.this.leveldbDirectory.getName();
        }
    }

    protected final class RocksDBType extends DBType<RocksDBKVDatabase> {

        private RocksDBType() {
            super(RocksDBKVDatabase.class);
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
            kvstore.setDirectory(AbstractMain.this.rocksdbDirectory);
            return kvstore;
        }

        @Override
        public String getDescription() {
            return "RocksDB " + AbstractMain.this.rocksdbDirectory.getName();
        }
    }

    protected final class ArrayDBType extends DBType<ArrayKVDatabase> {

        private ArrayDBType() {
            super(ArrayKVDatabase.class);
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
            kvstore.setDirectory(AbstractMain.this.arraydbDirectory);
            return kvstore;
        }

        @Override
        public String getDescription() {
            return "ArrayDB " + AbstractMain.this.arraydbDirectory.getName();
        }
    }

    protected final class MySQLDBType extends DBType<MySQLKVDatabase> {

        private MySQLDBType() {
            super(MySQLKVDatabase.class);
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
            mysql.setDataSource(new DriverManagerDataSource(AbstractMain.this.jdbcUrl));
            return mysql;
        }

        @Override
        public String getDescription() {
            return "MySQL";
        }
    }

    protected final class RaftDBType extends DBType<RaftKVDatabase> {

        private RaftDBType() {
            super(RaftKVDatabase.class);
        }

        @Override
        public RaftKVDatabase createKVDatabase() {

            // Setup network
            final TCPNetwork network = new TCPNetwork(RaftKVDatabase.DEFAULT_TCP_PORT);
            try {
                network.setListenAddress(AbstractMain.this.raftAddress != null ?
                  new InetSocketAddress(InetAddress.getByName(AbstractMain.this.raftAddress), AbstractMain.this.raftPort) :
                  new InetSocketAddress(AbstractMain.this.raftPort));
            } catch (UnknownHostException e) {
                throw new RuntimeException("can't resolve address `" + AbstractMain.this.raftAddress + "'", e);
            }

            // Set up Raft DB
            final RaftKVDatabase raft = new RaftKVDatabase();
            raft.setLogDirectory(AbstractMain.this.raftDirectory);
            raft.setKVStore(AbstractMain.this.raftKVStore);
            raft.setNetwork(network);
            raft.setIdentity(AbstractMain.this.raftIdentity);
            if (AbstractMain.this.raftMinElectionTimeout != -1)
                raft.setMinElectionTimeout(AbstractMain.this.raftMinElectionTimeout);
            if (AbstractMain.this.raftMaxElectionTimeout != -1)
                raft.setMaxElectionTimeout(AbstractMain.this.raftMaxElectionTimeout);
            if (AbstractMain.this.raftHeartbeatTimeout != -1)
                raft.setHeartbeatTimeout(AbstractMain.this.raftHeartbeatTimeout);

            // Done
            return raft;
        }

        @Override
        public String getDescription() {
            return "Raft " + AbstractMain.this.raftDirectory.getName();
        }
    }
}

