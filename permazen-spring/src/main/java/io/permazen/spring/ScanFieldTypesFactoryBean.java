
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

class ScanFieldTypesFactoryBean extends ScanClassPathFactoryBean {

    @Override
    AnnotatedClassScanner createScanner(ClassLoader loader) {
        return new PermazenFieldTypeScanner(loader, this.useDefaultFilters, this.environment);
    }
}
