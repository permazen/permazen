
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

/**
 * Parses <code>&lt;jsimpledb:scan-classes&gt;</code> tags.
 */
class ScanClassesBeanDefinitionParser extends ScanClassPathBeanDefinitionParser {

    @Override
    Class<?> getBeanClass() {
        return ScanClassesFactoryBean.class;
    }
}

