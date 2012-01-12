
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
 * @param <T> database transaction type
 */
public abstract class AbstractSchemaUpdate<T> implements SchemaUpdate<T> {

    private String name;
    private Set<SchemaUpdate<T>> requiredPredecessors = new HashSet<SchemaUpdate<T>>();
    private boolean singleAction;

    @Override
    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Set<SchemaUpdate<T>> getRequiredPredecessors() {
        return this.requiredPredecessors;
    }

    public void setRequiredPredecessors(Set<SchemaUpdate<T>> requiredPredecessors) {
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

