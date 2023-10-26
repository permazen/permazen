
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * Spring {@link org.springframework.beans.factory.xml.NamespaceHandler NamespaceHandler}
 * for the <code>permazen</code> XML namespace.
 *
 * <p>
 * This class adds support for the <code>&lt;permazen:permazen&gt;</code>
 * and <code>&lt;permazen:scan-classes&gt;</code> XML tags.
 *
 * @see io.permazen.spring
 */
public class PermazenNamespaceHandler extends NamespaceHandlerSupport {

    public static final String PERMAZEN_NAMESPACE_URI = "http://permazen.io/schema/spring/permazen";

    public static final String PERMAZEN_TAG = "permazen";
    public static final String SCAN_CLASSES_TAG = "scan-classes";

    @Override
    public void init() {
        this.registerBeanDefinitionParser(PERMAZEN_TAG, new PermazenBeanDefinitionParser());
        this.registerBeanDefinitionParser(SCAN_CLASSES_TAG, new ScanClassesBeanDefinitionParser());
    }
}
