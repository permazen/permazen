
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.dao;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.springframework.orm.jpa.JpaCallback;

/**
 * Helper class for building and executing JPA queries.
 *
 * @param <R> query return type
 */
public abstract class QueryCallback<R> implements JpaCallback<R> {

    @Override
    public final R doInJpa(EntityManager entityManager) {
        return this.executeQuery(this.buildQuery(entityManager));
    }

    /**
     * Build the query.
     */
    protected abstract Query buildQuery(EntityManager entityManager);

    /**
     * Execute the query.
     */
    protected abstract R executeQuery(Query query);
}

