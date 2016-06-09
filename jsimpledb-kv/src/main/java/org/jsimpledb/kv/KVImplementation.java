
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;

import javax.xml.stream.XMLStreamException;

import org.jsimpledb.kv.mvcc.AtomicKVDatabase;
import org.jsimpledb.kv.mvcc.AtomicKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Descriptor for a {@link KVDatabase} implementation.
 *
 * <p>
 * Instances of this class provide information about how to configure and instantiate some
 * technology-specific implementation of the {@link KVDatabase} and {@link AtomicKVStore}
 * interfaces.
 */
public abstract class KVImplementation {

    /**
     * Classpath XML file resource describing available {@link KVDatabase} implementations:
     * {@value #XML_DESCRIPTOR_RESOURCE}.
     *
     * <p>
     * Example:
     * <blockquote><code>
     *  &lt;kv-implementations&gt;
     *      &lt;kv-implementation class="com.example.MyKVImplementation"/&gt;
     *  &lt;/kv-implementations&gt;
     * </code></blockquote>
     *
     * <p>
     * Instances must have a public default constructor.
     */
    public static final String XML_DESCRIPTOR_RESOURCE = "META-INF/jsimpledb/kv-implementations.xml";

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Get the command line options supported by this implementation, suitable for display in a usage help message.
     *
     * <p>
     * There must be at least one option, to indicate that this implementation is to be used.
     *
     * @return array of pairs: { option, description }
     */
    public abstract String[][] getCommandLineOptions();

    /**
     * Parse the specified command line options and return the resulting configuration, if possible.
     *
     * <p>
     * At least one option must be recognized, indicating that this implementation is to be used,
     * otherwise null should be returned.
     *
     * @param options all command line options; upon return, recognized options should be removed
     * @return configuration object representing the parsed options, or null if this implementation is not specified/configured
     * @throws IllegalArgumentException if an option is recognized but invalid
     */
    public abstract Object parseCommandLineOptions(ArrayDeque<String> options);

    /**
     * Subclass support method for parsing out a single command line flag and argument.
     * If found, the {@code flag} and argument are removed from {@code options}.
     *
     * @param flag command line flag taking an argument
     * @return flag's argument, or null if flag is not present
     */
    protected String parseCommandLineOption(ArrayDeque<String> options, String flag) {
        String arg = null;
        for (Iterator<String> i = options.iterator(); i.hasNext(); ) {
            final String option = i.next();
            if (option.equals(flag)) {
                i.remove();
                if (!i.hasNext())
                    throw new IllegalArgumentException("`" + flag + "' missing required argument");
                arg = i.next();
                i.remove();
            }
        }
        return arg;
    }

    /**
     * Subclass support method for parsing out a single command line flag without argument.
     * If found, the {@code flag} is removed from {@code options}.
     *
     * @param flag command line flag taking an argument
     * @return whether flag is present
     */
    protected boolean parseCommandLineFlag(ArrayDeque<String> options, String flag) {
        boolean result = false;
        for (Iterator<String> i = options.iterator(); i.hasNext(); ) {
            final String option = i.next();
            if (option.equals(flag)) {
                result = true;
                i.remove();
            }
        }
        return result;
    }

    /**
     * Create an {@link KVDatabase} using the specified configuration.
     *
     * @param configuration implementation configuration returned by {@link #parseCommandLineOptions parseCommandLineOptions()}
     * @param kvdb required {@link KVDatabase}; will be null unless {@link #requiresKVDatabase} returned true
     * @param kvstore required {@link AtomicKVStore}; will be null unless {@link #requiresAtomicKVStore} returned true
     * @return new {@link KVDatabase} instance
     */
    public abstract KVDatabase createKVDatabase(Object configuration, KVDatabase kvdb, AtomicKVStore kvstore);

    /**
     * Create an {@link AtomicKVStore} using the specified configuration.
     *
     * <p>
     * The implementation in {@link KVImplementation} invokes {@link #createKVDatabase createKVDatabase()} and constructs
     * a {@link AtomicKVDatabase} from the result. Implementations that natively support the {@link AtomicKVDatabase}
     * interface should override this method.
     *
     * @param configuration implementation configuration returned by {@link #parseCommandLineOptions parseCommandLineOptions()}
     * @return new {@link AtomicKVStore} instance
     */
    public AtomicKVStore createAtomicKVStore(Object configuration) {
        return new AtomicKVDatabase(this.createKVDatabase(configuration, null, null));
    }

    /**
     * Determine whether this {@link KVDatabase} implementation requires an underlying {@link AtomicKVStore}.
     * If so, both implementations must be configured.
     *
     * <p>
     * The implementation in {@link KVImplementation} return false.
     *
     * @param configuration implementation configuration returned by {@link #parseCommandLineOptions parseCommandLineOptions()}
     * @return true if the implementation relies on an underlying {@link AtomicKVStore}
     */
    public boolean requiresAtomicKVStore(Object configuration) {
        return false;
    }

    /**
     * Determine whether this {@link KVDatabase} implementation requires some other underlying {@link KVDatabase}.
     * If so, both implementations must be configured.
     *
     * <p>
     * The implementation in {@link KVImplementation} return false.
     *
     * @param configuration implementation configuration returned by {@link #parseCommandLineOptions parseCommandLineOptions()}
     * @return true if the implementation relies on an underlying {@link KVDatabase}
     */
    public boolean requiresKVDatabase(Object configuration) {
        return false;
    }

    /**
     * Generate a short, human-readable description of the {@link KVDatabase} instance configured as given.
     *
     * @param configuration implementation configuration returned by {@link #parseCommandLineOptions parseCommandLineOptions()}
     */
    public abstract String getDescription(Object configuration);

    /**
     * Find all implementations on the classpath.
     */
    public static KVImplementation[] getImplementations() {

        // Get logger
        final Logger log = LoggerFactory.getLogger(KVImplementation.class);

        // Find XML files
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final Enumeration<URL> enumeration;
        try {
            enumeration = loader.getResources(XML_DESCRIPTOR_RESOURCE);
        } catch (IOException e) {
            log.error("error finding resources `" + XML_DESCRIPTOR_RESOURCE + "'", e);
            return new KVImplementation[0];
        }

        // Parse XML files and build a list of implementations
        final ArrayList<KVImplementation> list = new ArrayList<>();
        while (enumeration.hasMoreElements()) {
            final URL url = enumeration.nextElement();
            if (log.isDebugEnabled())
                log.debug("reading key/value implementations from " + url);
            try (final InputStream input = url.openStream()) {
                for (String className : new KVImplementationsReader(input).parse()) {
                    if (log.isDebugEnabled())
                        log.debug("instantiating key/value implementation " + className);
                    try {
                        list.add((KVImplementation)Class.forName(className, false, loader).newInstance());
                    } catch (Exception e) {
                        log.error("error instantiating class `" + className + "' specified in " + url, e);
                    }
                }
            } catch (IOException | XMLStreamException e) {
                log.error("error reading " + url, e);
                continue;
            }
        }

        // Done
        return list.toArray(new KVImplementation[list.size()]);
    }
}
