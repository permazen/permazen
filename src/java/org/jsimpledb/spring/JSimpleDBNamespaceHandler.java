
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * Spring {@link org.springframework.beans.factory.xml.NamespaceHandler NamespaceHandler}
 * for the <code>jsimpledb</code> XML namespace.
 *
 * <p>
 * This class adds support for the <code>&lt;jsimpledb:scan-classes&gt;</code> and
 * <code>&lt;jsimpledb:scan-field-types&gt;</code> XML tags.
 * </p>
 *
 * @see org.jsimpledb.spring
 */
public class JSimpleDBNamespaceHandler extends NamespaceHandlerSupport {

    @Override
    public void init() {
        this.registerBeanDefinitionParser("scan-classes", new ScanClassesBeanDefinitionParser());
        this.registerBeanDefinitionParser("scan-field-types", new ScanFieldTypesBeanDefinitionParser());
    }
}

