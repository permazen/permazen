
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.spring;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * Spring {@link org.springframework.beans.factory.xml.NamespaceHandler NamespaceHandler}
 * for the <code>jsimpledb</code> XML namespace.
 *
 * <p>
 * This class adds support for the <code>&lt;jsimpledb:jsimpledb&gt;</code>, <code>&lt;jsimpledb:scan-classes&gt;</code> and
 * <code>&lt;jsimpledb:scan-field-types&gt;</code> XML tags.
 *
 * @see org.jsimpledb.spring
 */
public class JSimpleDBNamespaceHandler extends NamespaceHandlerSupport {

    public static final String JSIMPLEDB_NAMESPACE_URI = "http://jsimpledb.googlecode.com/schema/jsimpledb";

    public static final String JSIMPLEDB_TAG = "jsimpledb";
    public static final String SCAN_CLASSES_TAG = "scan-classes";
    public static final String SCAN_FIELD_TYPES_TAG = "scan-field-types";

    @Override
    public void init() {
        this.registerBeanDefinitionParser(JSIMPLEDB_TAG, new JSimpleDBBeanDefinitionParser());
        this.registerBeanDefinitionParser(SCAN_CLASSES_TAG, new ScanClassesBeanDefinitionParser());
        this.registerBeanDefinitionParser(SCAN_FIELD_TYPES_TAG, new ScanFieldTypesBeanDefinitionParser());
    }
}

