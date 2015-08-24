
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
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

