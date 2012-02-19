
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;

import org.jibx.runtime.JiBXParseException;

public class Employee {

    private String name;

    public String getName() {
        return this.name;
    }
    public void setName(String name) {
        this.name = name;
    }

    // JiBX methods
    private String getJiBXId() {
        return IdMapper.getId(this);
    }
    private void setJiBXId(String id) {
        IdMapper.setId(this, id);
    }

    public static Employee deserializeEmployeeReference(String string) throws JiBXParseException {
        return ParseUtil.deserializeReference(string, Employee.class);
    }
}

