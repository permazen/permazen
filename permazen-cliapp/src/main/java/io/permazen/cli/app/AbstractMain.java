
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.app;

import com.google.common.base.Preconditions;

import io.permazen.PermazenFactory;
import io.permazen.core.Database;
import io.permazen.encoding.Encoding;
import io.permazen.encoding.EncodingRegistry;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.spring.PermazenClassScanner;
import io.permazen.spring.PermazenEncodingScanner;
import io.permazen.util.ApplicationClassLoader;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.common.cli.CommandLine;
import org.apache.common.cli.CommandLineParser;
import org.apache.common.cli.Option;
import org.apache.common.cli.Options;

/**
 * Support superclass for main entry point classes of Permazen-related applications.
 */
public abstract class AbstractMain {













    //private static final File DEMO_XML_FILE = new File("demo-database.xml");
    //private static final File DEMO_SUBDIR = new File("demo-classes");

    // Schema
    protected int schemaVersion;
    protected HashSet<Class<?>> schemaClasses;
    protected EncodingRegistry encodingRegistry;
    protected boolean allowNewSchema;

    // Key/value database
    protected KVDatabase kvdb;
    protected String databaseDescription;

    // Misc
    protected final ApplicationClassLoader loader = ApplicationClassLoader.getInstance();
    protected boolean showHelp;
    protected boolean verbose;
    protected boolean readOnly;

    // Key/value implementation(s) configuration
    private final HashMap<KVImplementation<?>, Object> kvConfigMap = new HashMap<>();
    private KVImplementation<?> requiredAtomicKVStore;
    private KVImplementation<?> requiredKVDatabase;
    private KVImplementation<?> kvImplementation;

    /**
     * Parse command line options.
     *
     * <p>
     * Upon return, {@code this.showHelp} will be true if the {@code --help} flag was given.
     * In that case the caller should invoke {@link #usageMessage} and then exit normally.
     * Otherwise, the {@code params} will contain the remaining non-option command line parameters.
     *
     * @param params command line parameters
     * @return true if sucessful, false if an error occured
     */
    public boolean parseOptions(ArrayDeque<String> params) {

        // Sanity check
        Preconditions.checkArgument(params != null, "null params");

        // Special logic to automate the demo when no flags (other than "--port XXXX") are given
        if (DEMO_XML_FILE.exists() && DEMO_SUBDIR.exists()
          && (params.isEmpty() || params.toString().matches("\\[--port, [0-9]+\\]"))) {
            params.add("--xml");
            params.add(DEMO_XML_FILE.toString());
            params.add("--classpath");
            params.add(DEMO_SUBDIR.toString());
            params.add("--model-pkg");
            params.add("io.permazen.demo");
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
                    return false;
                i.remove();
            }
        }

        // Parse (and remove) options supported by key/value implementations
        for (KVImplementation<?> availableKVImplementation : KVImplementation.getImplementations()) {
            final Object config;
            try {
                config = availableKVImplementation.parseCommandLineOptions(params);
            } catch (IllegalArgumentException e) {
                System.err.println(this.getName() + ": " + (e.getMessage() != null ? e.getMessage() : e));
                return false;
            }
            if (config != null)
                this.kvConfigMap.put(availableKVImplementation, config);
        }

        // Parse options supported by this class
        final LinkedHashSet<String> modelPackages = new LinkedHashSet<>();
        String encodingRegistryClass = null;
        while (!params.isEmpty() && params.peekFirst().startsWith("-")) {
            final String option = params.removeFirst();
            if (option.equals("-h") || option.equals("--help")) {
                this.showHelp = true;
                return true;
            } else if (option.equals("-ro") || option.equals("--read-only"))
                this.readOnly = true;
            else if (option.equals("--verbose"))
                this.verbose = true;
            else if (option.equals("-v") || option.equals("--schema-version")) {
                if (params.isEmpty())
                    this.usageError();
                final String vstring = params.removeFirst();
                if (vstring.trim().equalsIgnoreCase("auto")) {
                    this.schemaVersion = -1;
                    continue;
                }
                try {
                    this.schemaVersion = Integer.parseInt(vstring);
                    if (this.schemaVersion < -1)
                        throw new IllegalArgumentException("schema version is < -1");
                } catch (Exception e) {
                    System.err.println(this.getName() + ": invalid schema version \"" + vstring + "\": " + e.getMessage());
                    return false;
                }
            } else if (option.equals("--model-pkg")) {
                if (params.isEmpty())
                    this.usageError();
                modelPackages.add(params.removeFirst());
            } else if (option.equals("--encodings")) {
                if (params.isEmpty())
                    this.usageError();
                encodingRegistryClass = params.removeFirst();
            } else if (option.equals("-p") || option.equals("--pkg")) {
                if (params.isEmpty())
                    this.usageError();
                final String packageName = params.removeFirst();
                modelPackages.add(packageName);
            } else if (option.equals("--new-schema"))
                this.allowNewSchema = true;
            else if (option.equals("--"))
                break;
            else if (!this.parseOption(option, params)) {
                System.err.println(this.getName() + ": unknown option \"" + option + "\"");
                this.usageError();
                return false;
            }
        }

        // Decode what key/value implementations where specified and how they nest, if at all
        final Iterator<KVImplementation<?>> i = this.kvConfigMap.keySet().iterator();
        switch (this.kvConfigMap.size()) {
        case 0:
            System.err.println(this.getName() + ": no key/value database specified; use one of `--arraydb', etc.");
            this.usageError();
            return false;

        case 1:
            this.kvImplementation = i.next();
            final Object config = this.kvConfigMap.get(this.kvImplementation);
            if (this.requiresAtomicKVStore(this.kvImplementation, config)
              || this.requiresKVDatabase(this.kvImplementation, config)) {
                System.err.println(this.getName() + ": " + this.getDescription(this.kvImplementation, config)
                  + " requires the configuration of an underlying key/value technology; use one of `--arraydb', etc.");
                return false;
            }
            break;

        case 2:

            // Put them in proper order: inner first, outer second
            final KVImplementation<?>[] kvis = new KVImplementation<?>[] { i.next(), i.next() };
            final Object[] configs = new Object[] { this.kvConfigMap.get(kvis[0]), this.kvConfigMap.get(kvis[1]) };
            if (this.requiresAtomicKVStore(kvis[0], configs[0]) || this.requiresKVDatabase(kvis[0], configs[0])) {
                Collections.reverse(Arrays.asList(kvis));
                Collections.reverse(Arrays.asList(configs));
            }

            // Sanity check nesting requirements
            if ((this.requiresAtomicKVStore(kvis[0], configs[0]) || this.requiresKVDatabase(kvis[0], configs[0]))
              || !(this.requiresAtomicKVStore(kvis[1], configs[1]) || this.requiresKVDatabase(kvis[1], configs[1]))) {
                System.err.println(this.getName() + ": incompatible combination of " + this.getDescription(kvis[0], configs[0])
                  + " and " + this.getDescription(kvis[1], configs[1]));
                return false;
            }

            // Nest them as required
            if (this.requiresAtomicKVStore(kvis[1], configs[1]))
                this.requiredAtomicKVStore = kvis[0];
            else
                this.requiredKVDatabase = kvis[0];
            this.kvImplementation = kvis[1];
            break;

        default:
            System.err.println(this.getName() + ": too many key/value store(s) specified");
            return false;
        }

        // Scan for model and type classes
        final LinkedHashSet<String> emptyPackages = new LinkedHashSet<>();
        emptyPackages.addAll(modelPackages);
        modelPackages.stream().filter(this::scanModelClasses).forEach(emptyPackages::remove);

        // Warn about packages in which we didn't find any classes
        for (String packageName : emptyPackages)
            this.log.warn("no Java model classes found under package `{}'", packageName);

        // Instantiate custom EncodingRegistry
        if (encodingRegistryClass != null) {
            try {
                this.encodingRegistry = Class.forName(encodingRegistryClass,
                       false, Thread.currentThread().getContextClassLoader())
                      .asSubclass(EncodingRegistry.class).getConstructor().newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException("error instantiating class \"" + encodingRegistryClass + "\"", e);
            }
        }

        // Done
        return true;
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

    public PermazenFactory getPermazenFactory(Database db) {
        return new PermazenFactory()
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
     * @throws IllegalArgumentException if {@code path} is null
     */
    protected boolean appendClasspath(String path) {
        Preconditions.checkArgument(path != null, "null path");
        this.log.trace("adding classpath `{}' to application classpath", path);
        for (String file : path.split(System.getProperty("path.separator", ":"))) {
            if (file.length() == 0)
                continue;
            final URL url;
            try {
                url = new File(file).toURI().toURL();
            } catch (MalformedURLException e) {
                this.log.error("can't append `{}' to classpath: {}", file, e.toString(), e);
                return false;
            }
            this.loader.addURL(url);
            this.log.trace("added path component `{}' to classpath", file);
        }
        return true;
    }

    private boolean scanModelClasses(String pkgname) {
        if (this.schemaClasses == null)
            this.schemaClasses = new HashSet<>();
        final boolean[] foundAny = new boolean[1];
        new PermazenClassScanner(this.loader).scanForClasses(pkgname.split("[\\s,]")).stream()
          .peek(name -> this.log.debug("loading Java model class {}", name))
          .map(this::loadClass)
          .peek(cl -> foundAny[0] = true)
          .forEach(this.schemaClasses::add);
        return foundAny[0];
    }

    private boolean createDirectory(File dir) {
        if (!dir.exists() && !dir.mkdirs()) {
            System.err.println(this.getName() + ": could not create directory \"" + dir + "\"");
            return false;
        }
        if (!dir.isDirectory()) {
            System.err.println(this.getName() + ": file \"" + dir + "\" is not a directory");
            return false;
        }
        return true;
    }

    // Generic type futzing
    private <C> boolean requiresAtomicKVStore(KVImplementation<C> kvi, Object config) {
        return kvi.requiresAtomicKVStore(kvi.getConfigType().cast(config));
    }
    private <C> boolean requiresKVDatabase(KVImplementation<C> kvi, Object config) {
        return kvi.requiresKVDatabase(kvi.getConfigType().cast(config));
    }
    private <C> String getDescription(KVImplementation<C> kvi, Object config) {
        return kvi.getDescription(kvi.getConfigType().cast(config));
    }
    private <C> KVDatabase createKVDatabase(KVImplementation<C> kvi, Object config, KVDatabase kvdb, AtomicKVStore kvstore) {
        return kvi.createKVDatabase(kvi.getConfigType().cast(config), kvdb, kvstore);
    }
    private <C> AtomicKVStore createAtomicKVStore(KVImplementation<C> kvi, Object config) {
        return kvi.createAtomicKVStore(kvi.getConfigType().cast(config));
    }
    private <C> C getConfig(KVImplementation<C> kvi) {
        return kvi.getConfigType().cast(this.kvConfigMap.get(kvi));
    }

    /**
     * Load a class.
     *
     * @param className class name
     * @return class with name {@code className}
     * @throws RuntimeException if load fails
     * @throws IllegalArgumentException if {@code className} is null
     */
    protected Class<?> loadClass(String className) {
        Preconditions.checkArgument(className != null, "null className");
        try {
            return Class.forName(className, false, this.loader);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("failed to load class \"" + className + "\"", e);
        }
    }

    /**
     * Start the {@link Database} based on the configured {@link KVDatabase} and return it.
     *
     * @return initialized database
     */
    protected Database startupKVDatabase() {

        // Create database
        final Object config = this.getConfig(this.kvImplementation);
        final AtomicKVStore nestedKVS = this.requiredAtomicKVStore != null ?
          this.createAtomicKVStore(this.requiredAtomicKVStore, this.getConfig(this.requiredAtomicKVStore)) : null;
        final KVDatabase nestedKV = this.requiredKVDatabase != null ?
          this.createKVDatabase(this.requiredKVDatabase, this.getConfig(this.requiredAtomicKVStore), null, null) : null;
        this.kvdb = this.createKVDatabase(this.kvImplementation, config, nestedKV, nestedKVS);

        // Start up database
        this.databaseDescription = this.getDescription(this.kvImplementation, config);
        this.log.debug("using database: {}", this.databaseDescription);
        this.kvdb.start();

        // Construct core API Database
        final Database db = new Database(this.kvdb);

        // Register custom EncodingRegistry
        if (this.encodingRegistry != null)
            db.setEncodingRegistry(this.encodingRegistry);

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
            { "--schema-version, -v num",       "Specify schema version (default highest recorded; `auto' to auto-generate)" },
            { "--model-pkg package",            "Scan for @PermazenType model classes under Java package (=> Permazen mode)" },
            { "--encodings classname",          "Specify a custom EncodingRegistry to provide field encodings" },
            { "--pkg, -p package",              "Equivalent to `--model-pkg package --type-pkg package'" },
            { "--help, -h",                     "Show this help message" },
            { "--verbose",                      "Show verbose error messages" },
        }));

        // Add options supported by the various key/value implementations
        final List<KVImplementation<?>> kvs = KVImplementation.getImplementations();
        for (KVImplementation<?> kv : kvs)
            optionList.addAll(Arrays.<String[]>asList(kv.getCommandLineOptions()));

        // Add options supported by subclass
        if (subclassOpts != null)
            optionList.addAll(Arrays.<String[]>asList(subclassOpts));

        // Sort options
        Collections.sort(optionList, (opt1, opt2) -> opt1[0].compareTo(opt2[0]));

        // Display all supported options
        int width = 0;
        for (String[] opt : optionList)
            width = Math.max(width, opt[0].length());
        for (String[] opt : optionList)
            System.err.println(String.format("  %-" + width + "s  %s", opt[0], opt[1]));

        // Display additional usage text
        for (KVImplementation<?> kv : kvs) {
            final String usageText = kv.getUsageText();
            if (usageText != null)
                System.err.println(usageText.trim());
        }
    }

    /**
     * Add supported command line flags to the given list.
     *
     * <p>
     * Subclasses should override to add their own flags, 
}
