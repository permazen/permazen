
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import javax.validation.constraints.NotNull;

public class RootObject {

    private String name;
    private boolean verbose;

    @NotNull
    public String getName() {
        return this.name;
    }
    public void setName(String name) {
        this.name = name;
    }

    public boolean isVerbose() {
        return this.verbose;
    }
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        RootObject that = (RootObject)obj;
        return (this.name != null ? this.name.equals(that.name) : that.name == null)
          && this.verbose == that.verbose;
    }

    @Override
    public int hashCode() {
        return (this.name != null ? this.name.hashCode() : 0)
          ^ (this.verbose ? ~0 : 0);
    }
}

