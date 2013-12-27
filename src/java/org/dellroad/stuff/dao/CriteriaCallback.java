
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.dao;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;

/**
 * Specialization of {@link QueryCallback} for {@link CriteriaQuery} queries.
 *
 * @param <T> query result type, i.e., what the {@link CriteriaQuery} returns zero or more of
 * @param <R> query Java return type, usually either {@code R} or {@code List<R>}
 */
public abstract class CriteriaCallback<T, R> extends TypedQueryCallback<T, R> {

    protected final Class<T> type;

    protected CriteriaCallback(Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        this.type = type;
    }

    /**
     * Build the query by delegating to {@link #configureQuery configureQuery()} for configuration.
     */
    @Override
    protected TypedQuery<T> buildQuery(EntityManager entityManager) {
        final CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        final CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(this.type);
        this.configureQuery(criteriaQuery, criteriaBuilder);
        return entityManager.createQuery(criteriaQuery);
    }

    /**
     * Configure the {@link CriteriaQuery}.
     */
    protected abstract void configureQuery(CriteriaQuery<T> criteriaQuery, CriteriaBuilder criteriaBuilder);
}

