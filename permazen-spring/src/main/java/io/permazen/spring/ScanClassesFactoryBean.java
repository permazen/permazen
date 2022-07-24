
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

class ScanClassesFactoryBean extends ScanClassPathFactoryBean {

    @Override
    AnnotatedClassScanner createScanner(ClassLoader loader) {
        return new PermazenClassScanner(loader, this.useDefaultFilters, this.environment);
    }
}
