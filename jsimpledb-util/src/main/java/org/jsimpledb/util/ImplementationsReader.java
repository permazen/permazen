
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scans the classpath and parses XML descriptor files describing implementation classes.
 *
 * <p>
 * The XML files that this class parses look like this, for some value of {@code foobar}:
 *
 * <p>
 * Example:
 * <blockquote><pre>
 *  &lt;foobar-implementations&gt;
 *      &lt;foobar-implementation class="com.example.MyFoobarImplementation"/&gt;
 *  &lt;/foobar-implementations&gt;
 * </pre></blockquote>
 */
public class ImplementationsReader extends AbstractXMLStreaming {

    private static final QName CLASS_ATTR = new QName("class");

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final QName outerTag;
    private final QName innerTag;

    /**
     * Constructor.
     *
     * @param name base name for XML tags (e.g., {@code "foobar"})
     * @throws IllegalArgumentException if {@code name} is null
     */
    public ImplementationsReader(String name) {
        Preconditions.checkArgument(name != null && name.length() > 0, "null/empty name");
        this.outerTag = new QName(name + "-implementations");
        this.innerTag = new QName(name + "-implementation");
    }

    /**
     * Parse the XML input and return the list of class names.
     *
     * @param input XML input
     * @return list of class names
     * @throws IllegalArgumentException if {@code input} is null
     */
    public List<String> parse(InputStream input) throws XMLStreamException {
        Preconditions.checkArgument(input != null, "null input");
        final ArrayList<String> list = new ArrayList<>();
        final XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(input);
        this.expect(reader, false, this.outerTag);
        while (this.expect(reader, true, this.innerTag)) {
            list.add(this.getAttr(reader, CLASS_ATTR, true));
            this.expectClose(reader);
        }
        return list;
    }

    /**
     * Find all available implementations based on reading all XML files on the classpath at the specified resource name.
     *
     * <p>
     * If any errors occur, they are logged but otherwise ignored.
     *
     * @param type required implementation type
     * @param resource XML file classpath resource
     * @throws IllegalArgumentException if either parameter is null
     */
    public <T> List<T> findImplementations(Class<T> type, String resource) {

        // Sanity check
        Preconditions.checkArgument(type != null, "null type");
        Preconditions.checkArgument(resource != null, "null resource");

        // Find XML files
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        final Enumeration<URL> enumeration;
        try {
            enumeration = loader.getResources(resource);
        } catch (IOException e) {
            this.log.error("error finding resources `" + resource + "'", e);
            return Collections.emptyList();
        }

        // Parse XML files and build a list of implementations
        final ArrayList<T> list = new ArrayList<>();
        while (enumeration.hasMoreElements()) {
            final URL url = enumeration.nextElement();
            if (this.log.isDebugEnabled())
                this.log.debug("reading key/value implementations from " + url);
            try (final InputStream input = url.openStream()) {
                for (String className : this.parse(input)) {
                    if (this.log.isDebugEnabled())
                        this.log.debug("instantiating " + type.getSimpleName() + " implementation " + className);
                    try {
                        list.add(type.cast(Class.forName(className, false, loader).newInstance()));
                    } catch (Exception e) {
                        this.log.error("error instantiating class `" + className + "' specified in " + url, e);
                    }
                }
            } catch (IOException | XMLStreamException e) {
                this.log.error("error reading " + url, e);
                continue;
            }
        }

        // Done
        return list;
    }
}
