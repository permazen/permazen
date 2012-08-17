
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
 * @param <T> persistent instance type
 * @param <R> query return type
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
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(this.type);
        this.configureQuery(criteriaQuery, criteriaBuilder);
        return entityManager.createQuery(criteriaQuery);
    }

    /**
     * Configure the {@link CriteriaQuery}.
     */
    protected abstract void configureQuery(CriteriaQuery<T> criteriaQuery, CriteriaBuilder criteriaBuilder);
}

