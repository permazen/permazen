
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.dao;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TypedQuery;

/**
 * Helper class for building and executing JPA typed queries.
 *
 * @param <T> persistent instance type
 * @param <R> query return type
 */
public abstract class TypedQueryCallback<T, R> extends QueryCallback<R> {

    /**
     * Execute the query.
     *
     * <p>
     * The implementation in {@link TypedQueryCallback} delegates to {@link #executeQuery(TypedQuery)}.
     */
    @SuppressWarnings("unchecked")
    protected final R executeQuery(Query query) {
        return this.executeQuery((TypedQuery<T>)query);
    }

    /**
     * Build the query as a {@link TypedQuery}.
     */
    @Override
    protected abstract TypedQuery<T> buildQuery(EntityManager entityManager);

    /**
     * Execute the query.
     */
    protected abstract R executeQuery(TypedQuery<T> query);
}

