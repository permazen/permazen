
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
public abstract class AbstractSchemaUpdate implements ModifiableSchemaUpdate {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private String name;
    private Set<SchemaUpdate> requiredPredecessors = new HashSet<SchemaUpdate>();
    private boolean singleAction;

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Set<SchemaUpdate> getRequiredPredecessors() {
        return this.requiredPredecessors;
    }

    @Override
    public void setRequiredPredecessors(Set<SchemaUpdate> requiredPredecessors) {
        this.requiredPredecessors = requiredPredecessors;
    }

    @Override
    public boolean isSingleAction() {
        return this.singleAction;
    }

    @Override
    public void setSingleAction(boolean singleAction) {
        this.singleAction = singleAction;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + this.getName() + "]";
    }
}

