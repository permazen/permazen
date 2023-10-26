
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

/**
 * Represents an index on a simple field that is not a sub-fied of a complex field.
 */
class RegularSimpleFieldIndexInfo extends SimpleFieldIndexInfo {

    RegularSimpleFieldIndexInfo(JSimpleField jfield) {
        super(jfield);
        assert jfield.parent instanceof JClass;
    }
}
