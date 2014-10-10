
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

class JCounterFieldInfo extends JFieldInfo {

    JCounterFieldInfo(JCounterField jfield) {
        super(jfield);
    }

// Object

    @Override
    public String toString() {
        return "counter " + super.toString();
    }
}

