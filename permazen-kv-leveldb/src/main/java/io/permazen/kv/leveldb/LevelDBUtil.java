
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.leveldb;

import java.util.ArrayList;

import org.iq80.leveldb.DBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for use with LevelDB.
 */
public final class LevelDBUtil {

    /**
     * Class name for the {@link DBFactory} provided by <a href="https://github.com/fusesource/leveldbjni">leveldbjni</a>.
     */
    public static final String LEVELDBJNI_CLASS_NAME = "org.fusesource.leveldbjni.JniDBFactory";

    /**
     * Class name for the {@link DBFactory} provided by <a href="https://github.com/dain/leveldb">leveldb</a>.
     */
    public static final String LEVELDB_CLASS_NAME = "org.iq80.leveldb.impl.Iq80DBFactory";

    /**
     * The name of a system property that can be set to override the default {@link DBFactory} logic.
     * Set to the name of a class that implements {@link DBFactory} and has a zero-arg constructor.
     */
    public static final String DB_FACTORY_PROPERTY = LevelDBUtil.class.getName() + ".db_factory";

    private LevelDBUtil() {
    }

    /**
     * Get the default {@link DBFactory}.
     *
     * <p>
     * This method first tries the class specified by the {@link #DB_FACTORY_PROPERTY} system property, if any,
     * then <a href="https://github.com/fusesource/leveldbjni">leveldbjni</a>, and finally
     * <a href="https://github.com/dain/leveldb">leveldb</a>.
     *
     * @return {@link DBFactory} instance
     * @throws RuntimeException if no suitable {@link DBFactory} class can be found and instantiated
     * @see <a href="https://github.com/dain/leveldb">leveldb</a>
     * @see <a href="https://github.com/fusesource/leveldbjni">leveldbjni</a>
     */
    public static DBFactory getDefaultDBFactory() {

        // Get class names to try
        final ArrayList<String> classNames = new ArrayList<>(3);
        final String configuredFactoryClass = System.getProperty(DB_FACTORY_PROPERTY, null);
        if (configuredFactoryClass != null)
            classNames.add(configuredFactoryClass);
        classNames.add(LEVELDBJNI_CLASS_NAME);
        classNames.add(LEVELDB_CLASS_NAME);

        // Find class
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final Logger log = LoggerFactory.getLogger(LevelDBKVDatabase.class);
        for (String className : classNames) {
            try {
                return Class.forName(className, false, loader).asSubclass(DBFactory.class).getConstructor().newInstance();
            } catch (Exception e) {
                if (log.isDebugEnabled())
                    log.debug("can't load factory class \"" + className + "\": " + e);
                continue;
            }
        }

        // Nothing found
        throw new RuntimeException(String.format("no %s implementation found; tried: %s", DBFactory.class.getName(), classNames));
    }
}
