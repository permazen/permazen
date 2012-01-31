
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;

public class Employee {

    private String name;

    public String getName() {
        return this.name;
    }
    public void setName(String name) {
        this.name = name;
    }

    // JiBX methods
    private String getJiBXID() {
        return IdMapper.getId(this);
    }
    private void setJiBXID(String id) {
        // do nothing
    }
}

