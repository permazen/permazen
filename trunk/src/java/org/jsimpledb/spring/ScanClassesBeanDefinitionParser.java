
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

/**
 * Parses <code>&lt;jsimpledb:scan-classes&gt;</code> tags.
 */
class ScanClassesBeanDefinitionParser extends ScanClassPathBeanDefinitionParser {

    @Override
    Class<?> getBeanClass() {
        return ScanClassesFactoryBean.class;
    }
}

