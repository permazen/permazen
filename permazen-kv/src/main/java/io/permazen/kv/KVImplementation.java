
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import com.google.common.base.Preconditions;

import io.permazen.kv.mvcc.AtomicKVDatabase;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.util.ImplementationsReader;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Descriptor for a {@link KVDatabase} implementation.
 *
 * <p>
 * Instances of this class provide information about how to configure and instantiate some
 * technology-specific implementation of the {@link KVDatabase} and {@link AtomicKVStore}
 * interfaces.
 *
 * @param <C> configuration object type
 */
public abstract class KVImplementation<C> {

    /**
     * Classpath XML file resource describing available {@link KVDatabase} implementations:
     * {@value #XML_DESCRIPTOR_RESOURCE}.
     *
     * <p>
     * Example:
     * <blockquote><pre>
     *  &lt;kv-implementations&gt;
     *      &lt;kv-implementation class="com.example.MyKVImplementation"/&gt;
     *  &lt;/kv-implementations&gt;
     * </pre></blockquote>
     *
     * <p>
     * Instances must have a public default constructor.
     */
    public static final String XML_DESCRIPTOR_RESOURCE = "META-INF/permazen/kv-implementations.xml";

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Class<C> configType;

    /**
     * Constructor.
     *
     * @param configType configuration object type
     */
    protected KVImplementation(Class<C> configType) {
        Preconditions.checkArgument(configType != null, "null configType");
        this.configType = configType;
    }

    /**
     * Get the configuration object type.
     *
     * @return config type
     */
    public Class<C> getConfigType() {
        return this.configType;
    }

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
     * Get additional usage message text, if any.
     *
     * <p>
     * The implementation in {@link KVImplementation} returns null.
     *
     * @return usage text if any, otherwise null
     */
    public String getUsageText() {
        return null;
    }

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
    public abstract C parseCommandLineOptions(ArrayDeque<String> options);

    /**
     * Subclass support method for parsing out a single command line flag and argument.
     * If found, the {@code flag} and argument are removed from {@code options}.
     *
     * @param options command line options
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
                    throw new IllegalArgumentException("\"" + flag + "\" missing required argument");
                arg = i.next();
                i.remove();
            }
        }
        return arg;
    }

    /**
     * Subclass support method for parsing out a boolean command line flag (i.e., a flag without an argument).
     * If found, all instances of {@code flag} are removed from {@code options}.
     *
     * @param options command line options
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
    public abstract KVDatabase createKVDatabase(C configuration, KVDatabase kvdb, AtomicKVStore kvstore);

    /**
     * Create an {@link AtomicKVStore} using the specified configuration.
     *
     * <p>
     * The implementation in {@link KVImplementation} invokes {@link #createKVDatabase createKVDatabase()} and constructs
     * an {@link AtomicKVDatabase} from the result. Implementations that natively support the {@link AtomicKVDatabase}
     * interface should override this method.
     *
     * @param configuration implementation configuration returned by {@link #parseCommandLineOptions parseCommandLineOptions()}
     * @return new {@link AtomicKVStore} instance
     */
    public AtomicKVStore createAtomicKVStore(C configuration) {
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
    public boolean requiresAtomicKVStore(C configuration) {
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
    public boolean requiresKVDatabase(C configuration) {
        return false;
    }

    /**
     * Generate a short, human-readable description of the {@link KVDatabase} instance configured as given.
     *
     * @param configuration implementation configuration returned by {@link #parseCommandLineOptions parseCommandLineOptions()}
     * @return human-readable description
     */
    public abstract String getDescription(C configuration);

    /**
     * Find available {@link KVImplementation}s by scanning the classpath.
     *
     * <p>
     * This method searches the classpath for {@link KVImplementation} descriptor files and instantiates
     * the corresponding {@link KVImplementation}s. Example:
     * <blockquote><pre>
     *  &lt;kv-implementations&gt;
     *      &lt;kv-implementation class="com.example.MyKVImplementation"/&gt;
     *  &lt;/kv-implementations&gt;
     * </pre></blockquote>
     *
     * @return {@link KVImplementation}s found on the classpath
     */
    @SuppressWarnings("unchecked")
    public static List<KVImplementation<?>> getImplementations() {
        return (List<KVImplementation<?>>)(Object)new ImplementationsReader("kv").findImplementations(
          KVImplementation.class, XML_DESCRIPTOR_RESOURCE);
    }
}
