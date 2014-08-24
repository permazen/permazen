
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

class ScanClassesFactoryBean extends ScanClassPathFactoryBean {

    @Override
    AnnotatedClassScanner createScanner() {
        return new JSimpleDBClassScanner(this.useDefaultFilters, this.environment);
    }
}

