
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.util.HashSet;
import java.util.Set;

/**
 * Support superclass for {@link SchemaUpdate} implementations with standard bean property implementations.
 *
 * @param <C> database connection type
 */
public abstract class AbstractSchemaUpdate<C> implements SchemaUpdate<C> {

    private String name;
    private Set<SchemaUpdate<C>> requiredPredecessors = new HashSet<SchemaUpdate<C>>();
    private boolean singleAction;

    @Override
    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Set<SchemaUpdate<C>> getRequiredPredecessors() {
        return this.requiredPredecessors;
    }

    public void setRequiredPredecessors(Set<SchemaUpdate<C>> requiredPredecessors) {
        this.requiredPredecessors = requiredPredecessors;
    }

    @Override
    public boolean isSingleAction() {
        return this.singleAction;
    }

    public void setSingleAction(boolean singleAction) {
        this.singleAction = singleAction;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + this.getName() + "]";
    }
}

