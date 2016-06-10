
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.app;

import com.google.common.base.Function;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;

import org.dellroad.stuff.main.MainClass;
import org.jsimpledb.JSimpleDBFactory;
import org.jsimpledb.annotation.JFieldType;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.KVImplementation;
import org.jsimpledb.kv.mvcc.AtomicKVStore;
import org.jsimpledb.spring.JSimpleDBClassScanner;
import org.jsimpledb.spring.JSimpleDBFieldTypeScanner;

/**
 * Support superclass for main entry point classes of JSimpleDB-related applications.
 */
public abstract class AbstractMain extends MainClass {

    private static final File DEMO_XML_FILE = new File("demo-database.xml");
    private static final File DEMO_SUBDIR = new File("demo-classes");

    // Schema
    protected int schemaVersion;
    protected HashSet<Class<?>> schemaClasses;
    protected HashSet<Class<? extends FieldType<?>>> fieldTypeClasses;
    protected boolean allowNewSchema;

    // Key/value database
    protected KVDatabase kvdb;
    protected String databaseDescription;

    // Misc
    protected boolean verbose;
    protected boolean readOnly;

    // Key/value implementation(s) configuration
    private final HashMap<KVImplementation, Object> kvConfigMap = new HashMap<>();
    private KVImplementation requiredAtomicKVStore;
    private KVImplementation requiredKVDatabase;
    private KVImplementation kvImplementation;

    /**
     * Parse command line options.
     *
     * @param params command line parameters
     * @return -1 to proceed, otherwise process exit value
     */
    public int parseOptions(ArrayDeque<String> params) {

        // Special logic to automate the demo when no flags are given
        if (params.isEmpty() && DEMO_XML_FILE.exists() && DEMO_SUBDIR.exists()) {
            params.add("--xml");
            params.add(DEMO_XML_FILE.toString());
            params.add("--classpath");
            params.add(DEMO_SUBDIR.toString());
            params.add("--model-pkg");
            params.add("org.jsimpledb.demo");
            final StringBuilder buf = new StringBuilder();
            for (String param : params)
                buf.append(' ').append(param);
            System.err.println(this.getName() + ": automatically configuring demo database using the following flags:\n " + buf);
        }

        // Parse --classpath options prior to searching classpath for key/value implementations
        for (Iterator<String> i = params.iterator(); i.hasNext(); ) {
            final String param = i.next();
            if (param.equals("-cp") || param.equals("--classpath")) {
                i.remove();
                if (!i.hasNext())
                    this.usageError();
                if (!this.appendClasspath(i.next()))
                    return 1;
                i.remove();
            }
        }

        // Parse (and remove) options supported by key/value implementations
        for (KVImplementation availableKVImplementation : KVImplementation.getImplementations()) {
            final Object config;
            try {
                config = availableKVImplementation.parseCommandLineOptions(params);
            } catch (IllegalArgumentException e) {
                System.err.println(this.getName() + ": " + (e.getMessage() != null ? e.getMessage() : e));
                return 1;
            }
            if (config != null)
                this.kvConfigMap.put(availableKVImplementation, config);
        }

        // Parse options supported by this class
        final LinkedHashSet<String> modelPackages = new LinkedHashSet<>();
        final LinkedHashSet<String> typePackages = new LinkedHashSet<>();
        while (!params.isEmpty() && params.peekFirst().startsWith("-")) {
            final String option = params.removeFirst();
            if (option.equals("-h") || option.equals("--help")) {
                this.usageMessage();
                return 0;
            } else if (option.equals("-ro") || option.equals("--read-only"))
                this.readOnly = true;
            else if (option.equals("--verbose"))
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
            } else if (option.equals("--new-schema"))
                this.allowNewSchema = true;
            else if (option.equals("--"))
                break;
            else if (!this.parseOption(option, params)) {
                System.err.println(this.getName() + ": unknown option `" + option + "'");
                this.usageError();
                return 1;
            }
        }

        // Decode what key/value implementations where specified and how they nest, if at all
        final Iterator<KVImplementation> i = this.kvConfigMap.keySet().iterator();
        switch (this.kvConfigMap.size()) {
        case 0:
            System.err.println(this.getName() + ": no key/value database specified; use one of `--arraydb', etc.");
            this.usageError();
            return 1;

        case 1:
            this.kvImplementation = i.next();
            final Object config = this.kvConfigMap.get(this.kvImplementation);
            if (this.kvImplementation.requiresAtomicKVStore(config) || this.kvImplementation.requiresKVDatabase(config)) {
                System.err.println(this.getName() + ": " + this.kvImplementation.getDescription(config)
                  + " requires the configuration of an underlying key/value technology; use one of `--arraydb', etc.");
                return 1;
            }
            break;

        case 2:

            // Put them in proper order: inner first, outer second
            final KVImplementation[] kvis = new KVImplementation[] { i.next(), i.next() };
            final Object[] configs = new Object[] { this.kvConfigMap.get(kvis[0]), this.kvConfigMap.get(kvis[1]) };
            if (kvis[0].requiresAtomicKVStore(configs[0]) || kvis[0].requiresKVDatabase(configs[0])) {
                Collections.reverse(Arrays.asList(kvis));
                Collections.reverse(Arrays.asList(configs));
            }

            // Sanity check nesting requirements
            if ((kvis[0].requiresAtomicKVStore(configs[0]) || kvis[0].requiresKVDatabase(configs[0]))
              || !(kvis[1].requiresAtomicKVStore(configs[1]) || kvis[1].requiresKVDatabase(configs[1]))) {
                System.err.println(this.getName() + ": incompatible combination of " + kvis[0].getDescription(configs[0])
                  + " and " + kvis[1].getDescription(configs[1]));
                return 1;
            }

            // Nest them as required
            if (kvis[1].requiresAtomicKVStore(configs[1]))
                this.requiredAtomicKVStore = kvis[0];
            else
                this.requiredKVDatabase = kvis[0];
            this.kvImplementation = kvis[1];
            break;

        default:
            System.err.println(this.getName() + ": too many key/value store(s) specified");
            return 1;
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

        // Create database
        final Object config = this.kvConfigMap.get(this.kvImplementation);
        final AtomicKVStore nestedKVS = this.requiredAtomicKVStore != null ?
          this.requiredAtomicKVStore.createAtomicKVStore(this.kvConfigMap.get(this.requiredAtomicKVStore)) : null;
        final KVDatabase nestedKV = this.requiredKVDatabase != null ?
          this.requiredKVDatabase.createKVDatabase(this.kvConfigMap.get(this.requiredAtomicKVStore), null, null) : null;
        this.kvdb = this.kvImplementation.createKVDatabase(config, nestedKV, nestedKVS);

        // Start up database
        this.databaseDescription =  this.kvImplementation.getDescription(config);
        this.log.debug("using database: " + this.databaseDescription);
        this.kvdb.start();

        // Construct core API Database
        final Database db = new Database(this.kvdb);

        // Register custom field types
        if (this.fieldTypeClasses != null)
            db.getFieldTypeRegistry().addClasses(this.fieldTypeClasses);

        // Done
        return db;
    }

    /**
     * Shutdown the {@link KVDatabase}.
     */
    protected void shutdownKVDatabase() {
        this.kvdb.stop();
    }

    protected abstract String getName();

    /**
     * Output usage message flag listing.
     *
     * @param subclassOpts array containing flag and description pairs
     */
    protected void outputFlags(String[][] subclassOpts) {

        // Build options list
        final ArrayList<String[]> optionList = new ArrayList<>();

        // Add options directly supported by AbstractMain
        optionList.addAll(Arrays.<String[]>asList(new String[][] {
            { "--classpath, -cp path",          "Append to the classpath (useful with `java -jar ...')" },
            { "--read-only, -ro",               "Disallow database modifications" },
            { "--new-schema",                   "Allow recording of a new database schema version" },
            { "--schema-version, -v num",       "Specify database schema version (default highest recorded)" },
            { "--model-pkg package",            "Scan for @JSimpleClass model classes under Java package (=> JSimpleDB mode)" },
            { "--type-pkg package",             "Scan for @JFieldType types under Java package to register custom types" },
            { "--pkg, -p package",              "Equivalent to `--model-pkg package --type-pkg package'" },
            { "--help, -h",                     "Show this help message" },
            { "--verbose",                      "Show verbose error messages" },
        }));

        // Add options supported by the various key/value implementations
        final KVImplementation[] kvs = KVImplementation.getImplementations();
        for (KVImplementation kv : kvs)
            optionList.addAll(Arrays.<String[]>asList(kv.getCommandLineOptions()));

        // Add options supported by subclass
        if (subclassOpts != null)
            optionList.addAll(Arrays.<String[]>asList(subclassOpts));

        // Sort options
        Collections.sort(optionList, new Comparator<String[]>() {
            @Override
            public int compare(String[] opt1, String[] opt2) {
                return opt1[0].compareTo(opt2[0]);
            }
        });

        // Display all supported options
        int width = 0;
        for (String[] opt : optionList)
            width = Math.max(width, opt[0].length());
        for (String[] opt : optionList)
            System.err.println(String.format("  %-" + width + "s  %s", opt[0], opt[1]));

        // Display additional usage text
        for (KVImplementation kv : kvs) {
            final String usageText = kv.getUsageText();
            if (usageText != null)
                System.err.println(usageText.trim());
        }
    }
}
