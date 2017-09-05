
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

class ScanFieldTypesFactoryBean extends ScanClassPathFactoryBean {

    @Override
    AnnotatedClassScanner createScanner() {
        return new JSimpleDBFieldTypeScanner(this.useDefaultFilters, this.environment);
    }
}

