
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.demo;

/**
 * Support superclass for {@link Body} implementations.
 */
public abstract class AbstractBody implements Body {

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "@" + this.getObjId();
    }
}

