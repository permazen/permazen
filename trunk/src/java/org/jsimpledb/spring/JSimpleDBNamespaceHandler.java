
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
 * This class adds support for the <code>&lt;jsimpledb:jlayer-scan&gt;</code> XML tag.
 * </p>
 *
 * @see org.jsimpledb.spring
 */
public class JSimpleDBNamespaceHandler extends NamespaceHandlerSupport {

    @Override
    public void init() {
        this.registerBeanDefinitionParser("jlayer-scan", new JLayerScanBeanDefinitionParser());
    }
}

