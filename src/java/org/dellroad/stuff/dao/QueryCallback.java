
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.dao;

import javax.persistence.EntityManager;
import javax.persistence.Query;

/**
 * Helper class for building and executing JPA queries.
 *
 * @param <R> query return type
 */
public abstract class QueryCallback<R> {

    /**
     * Perform the query and return its result.
     *
     * <p>
     * The implementation in {@link QueryCallback} simply delegates to {@link #buildQuery} and then {@link #executeQuery}.
     * Subclasses may override if desired.
     *
     * @param entityManager JPA {@link EntityManager} representing an open transaction
     * @return result of query
     */
    public R query(EntityManager entityManager) {
        return this.executeQuery(this.buildQuery(entityManager));
    }

    /**
     * Build the query.
     *
     * @param entityManager JPA {@link EntityManager} representing an open transaction
     * @return constructed JPA {@link Query} to perform
     */
    protected abstract Query buildQuery(EntityManager entityManager);

    /**
     * Execute the query.
     *
     * @param query the {@link Query} constructed by {@link #buildQuery}
     * @return result of query
     */
    protected abstract R executeQuery(Query query);
}

