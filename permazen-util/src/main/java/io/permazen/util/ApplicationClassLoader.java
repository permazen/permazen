
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * An {@link URLClassLoader} whose classpath can be modified at runtime.
 *
 * <p>
 * This is intended for use by applications that want to load classes using the usual system or context class loader, but need
 * the ability to add {@link URL}s to the classpath that is searched. In JDK 9 and later, the system class loader can no longer
 * be directly modified (this previously used an ugly reflection hack anyway). This class effectively restores that capability,
 * by overriding {@link #addURL addURL()} and making it public.
 *
 * <p>
 * This class also provides a way to lookup existing instances by parent; see {@link #getInstance}. These instances
 * are cached using weak references to avoid a memory leak.
 *
 * <p>
 * Instances work together to behave like a singleton; an {@link URL} added to any instance by {@link #addURL addURL()} is
 * automagically added to all existing and future instances.
 */
public final class ApplicationClassLoader extends URLClassLoader {

    private static final Map<ClassLoader, ApplicationClassLoader> INSTANCES = new MapMaker().weakKeys().makeMap();
    private static final LinkedHashSet<URI> EXTRA_URIS = new LinkedHashSet<>();     // sort URI's to avoid terrible URL.equals()

// Constructors

    /**
     * Constructor.
     *
     * @param parent parent class loader, or null for the boot class loader
     */
    private ApplicationClassLoader(ClassLoader parent) {
        super(new URL[0], parent);
        synchronized (ApplicationClassLoader.class) {
            EXTRA_URIS.forEach(this::addURI);
        }
    }

// URLClassLoader

    /**
     * Append the specified file to the search classpath.
     *
     * @param file classpath component
     * @throws IllegalArgumentException if {@code file} is null
     */
    public void addFile(File file) {
        Preconditions.checkArgument(file != null, "null file");
        this.addURL(this.toURL(file.toURI()));
    }

    /**
     * {@inheritDoc}
     *
     * @param url the URL to be added to the search path of URLs
     * @throws IllegalArgumentException if {@code url} cannot be {@linkplain URL#toURI converted} into an {@link URI}
     * @throws IllegalArgumentException if {@code url} is null
     */
    @Override
    public void addURL(URL url) {

        // Get URI form of URL
        final URI uri = this.toURI(url);

        // Add to all extant instances (if new)
        synchronized (ApplicationClassLoader.class) {
            if (EXTRA_URIS.add(uri))
                INSTANCES.values().forEach(loader -> loader.addURI(uri));
        }
    }

    private void addURI(URI uri) {
        super.addURL(this.toURL(uri));
    }

    private URL toURL(URI uri) {
        Preconditions.checkArgument(uri != null, "null uri");
        try {
            return uri.toURL();
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(String.format("can't convert %s \"%s\" into an %s", "URI", uri, "URL"), e);
        }
    }

    private URI toURI(URL url) {
        Preconditions.checkArgument(url != null, "null url");
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(String.format("can't convert %s \"%s\" into an %s", "URL", url, "URI"), e);
        }
    }

/*
    @Override
    public URL findResource(String name) {
        final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(this.getClass());
        log.debug("*** {}.findResource(\"{}\"): start", this, name);
        try {
            final URL url = super.findResource(name);
            log.debug("*** {}.findResource(\"{}\"): FOUND: {}", this, name, url);
            return url;
        } catch (RuntimeException e) {
            log.debug("*** {}.findResource(\"{}\"): FAILED: {}", this, name, e.toString());
            throw e;
        }
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(this.getClass());
        log.debug("*** {}.findClass(\"{}\"): start", this, name);
        try {
            final Class<?> cl = super.findClass(name);
            log.debug("*** {}.findClass(\"{}\"): FOUND: {}", this, name, cl);
            return cl;
        } catch (ClassNotFoundException | RuntimeException e) {
            log.debug("*** {}.findClass(\"{}\"): FAILED: {}", this, name, e.toString());
            throw e;
        }
    }
*/

// Methods

    /**
     * Obtain the unique instance having the current thread's {@linkplain Thread#getContextClassLoader context class loader}
     * as its parent, creating it on demand if needed.
     */
    public static ApplicationClassLoader getInstance() {
        return ApplicationClassLoader.getInstance(Thread.currentThread().getContextClassLoader());
    }

    /**
     * Obtain the unique instance having the given {@link ClassLoader} as its parent, creating it on demand if needed.
     *
     * @param parent parent class loader, or null for the boot class loader
     * @return corresponding {@link ApplicationClassLoader}, or {@code parent} if {@code parent}
     *  is itself already an {@link ApplicationClassLoader}
     */
    public static synchronized ApplicationClassLoader getInstance(ClassLoader parent) {
        if (parent instanceof ApplicationClassLoader)
            return (ApplicationClassLoader)parent;
        synchronized (ApplicationClassLoader.class) {
            return INSTANCES.computeIfAbsent(parent, ApplicationClassLoader::new);
        }
    }
}
