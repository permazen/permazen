
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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

    private final ArrayList<Object[]> constructorParamList = new ArrayList<>();

    private final String name;
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
        this.constructorParamList.add(new Object[0]);
        this.name = name;
    }

    /**
     * Configure constructor parameter lists.
     *
     * <p>
     * By default, only default constructors are tried. This method allows configuring a list of
     * possible constructors to try, by supplying corresponding parameter lists.
     * The parameter lists will be tried in order.
     *
     * <p>
     * If multiple constructors are compatible with a given parameter list, which one gets chosen
     * is indeterminate.
     *
     * @param list list of constructor parameter lists to try
     * @throws IllegalArgumentException if {@code list} is null
     * @throws IllegalArgumentException if {@code list} is empty
     * @throws IllegalArgumentException if {@code list} contains a null entry
     */
    public void setConstructorParameterLists(List<Object[]> list) {
        Preconditions.checkArgument(list != null, "null list");
        Preconditions.checkArgument(!list.isEmpty(), "empty list");
        for (Object[] params : list)
            Preconditions.checkArgument(params != null, "null params");
        this.constructorParamList.clear();
        this.constructorParamList.addAll(list);
    }

    /**
     * Parse the XML input and return the list of class names.
     *
     * @param input XML input
     * @return list of class names
     * @throws IllegalArgumentException if {@code input} is null
     * @throws XMLStreamException if XML parse fails
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
     * @return list of instantiated implementations
     * @param <T> implementation type
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
                this.log.debug("reading " + this.name + " implementations from " + url);
            try (InputStream input = url.openStream()) {
              classLoop:
                for (String className : this.parse(input)) {

                    // Get class
                    if (this.log.isDebugEnabled())
                        this.log.debug("loading " + type.getSimpleName() + " implementation " + className);
                    final Class<? extends T> klass;
                    try {
                        klass = Class.forName(className, false, loader).asSubclass(type);
                    } catch (Exception e) {
                        this.log.error("error loading class `" + className + "' specified in " + url, e);
                        continue;
                    }

                    // Instantiate class
                    if (this.log.isDebugEnabled())
                        this.log.debug("instantiating " + type.getSimpleName() + " implementation " + className);
                    final Constructor<?>[] constructors = klass.getConstructors();
                    Throwable error = null;
                    for (Object[] params : this.constructorParamList) {
                        for (Constructor<?> constructor : constructors) {
                            final Class<?>[] ptypes = constructor.getParameterTypes();
                            if (ptypes.length != params.length)
                                continue;
                            for (int i = 0; i < ptypes.length; i++) {
                                if (ptypes[i].isPrimitive() && params[i] == null)
                                    continue;
                                if (!TypeToken.of(ptypes[i]).wrap().getRawType().isInstance(params[i]))
                                    continue;
                            }
                            try {
                                list.add(type.cast(constructor.newInstance(params)));
                                continue classLoop;
                            } catch (Exception e) {
                               error = e instanceof InvocationTargetException ? e.getCause() : e;
                            }
                        }
                    }

                    // Report the error
                    if (error == null) {
                        this.log.error("error instantiating class `" + className + "' specified in " + url
                          + ": no suitable constructor found");
                    } else
                        this.log.error("error instantiating class `" + className + "' specified in " + url, error);
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
