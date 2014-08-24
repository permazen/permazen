
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

/**
 * Parses <code>&lt;jsimpledb:scan-field-types&gt;</code> tags.
 */
class ScanFieldTypesBeanDefinitionParser extends ScanClassPathBeanDefinitionParser {

    @Override
    Class<?> getBeanClass() {
        return ScanFieldTypesFactoryBean.class;
    }
}

