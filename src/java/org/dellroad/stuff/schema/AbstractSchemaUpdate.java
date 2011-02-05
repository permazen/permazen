
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support superclass for {@link SchemaUpdate}s implementations providing standard Java bean property
 * implementations of {@link #getName} and {@link #getRequiredPredecessors}.
 */
public abstract class AbstractSchemaUpdate implements SchemaUpdate {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private String name;
    private Set<SchemaUpdate> requiredPredecessors = new HashSet<SchemaUpdate>();

    @Override
    public String getName() {
        return this.name;
    }
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Set<SchemaUpdate> getRequiredPredecessors() {
        return this.requiredPredecessors;
    }
    public void setRequiredPredecessors(Set<SchemaUpdate> requiredPredecessors) {
        this.requiredPredecessors = requiredPredecessors;
    }
}

