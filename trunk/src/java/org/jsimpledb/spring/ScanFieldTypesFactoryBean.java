
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

class ScanFieldTypesFactoryBean extends ScanClassPathFactoryBean {

    @Override
    AnnotatedClassScanner createScanner() {
        return new JSimpleDBFieldTypeScanner(this.useDefaultFilters, this.environment);
    }
}

