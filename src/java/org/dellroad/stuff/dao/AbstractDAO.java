
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.dao;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.jpa.JpaCallback;
import org.springframework.orm.jpa.JpaTemplate;
import org.springframework.orm.jpa.support.JpaDaoSupport;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Support superclass for JPA DAO implementations.
 *
 * @param <T> persistent instance type
 */
public abstract class AbstractDAO<T> extends JpaDaoSupport implements DAO<T> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Persistent instance type.
     */
    protected final Class<T> type;

    /**
     * Constructor.
     *
     * @param type persistent instance type
     */
    protected AbstractDAO(Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        this.type = type;
    }

    /**
     * Constructor.
     *
     * @param type persistent instance type
     * @param entityManagerFactory {@link EntityManagerFactory} from which to create the {@link JpaTemplate} used by this instance
     */
    protected AbstractDAO(Class<T> type, EntityManagerFactory entityManagerFactory) {
        this(type);
        this.setEntityManagerFactory(entityManagerFactory);
    }

    /**
     * Constructor.
     *
     * @param type persistent instance type
     * @param jpaTemplate {@link JpaTemplate} to be used by this instance
     */
    protected AbstractDAO(Class<T> type, JpaTemplate jpaTemplate) {
        this(type);
        this.setJpaTemplate(jpaTemplate);
    }

// Access methods

    @Override
    public T getById(long id) {
        return this.getJpaTemplate().find(this.type, id);
    }

    @Override
    public List<T> getAll() {
        return this.getBy(new DAOCriteriaListCallback() {
            @Override
            protected void configureQuery(CriteriaQuery<T> criteriaQuery, CriteriaBuilder criteriaBuilder) {
                // no criteria - we want them all
            }
        });
    }

    @Override
    public T getReference(long id) {
        return this.getJpaTemplate().getReference(this.type, id);
    }

    /**
     * Find instances using a query string and query parameters.
     */
    protected List<T> find(final String queryString, final Object... params) {
        return this.getBy(new DAOQueryListCallback() {
            @Override
            protected TypedQuery<T> buildQuery(EntityManager entityManager) {
                return AbstractDAO.this.buildQuery(entityManager, queryString, params);
            }
        });
    }

    /**
     * Find a unique instance using a query string and query parameters.
     */
    protected T findUnique(final String queryString, final Object... params) {
        return this.getBy(new DAOQueryUniqueCallback() {
            @Override
            protected TypedQuery<T> buildQuery(EntityManager entityManager) {
                return AbstractDAO.this.buildQuery(entityManager, queryString, params);
            }
        });
    }

    /**
     * Get the unique instance matched by the given query callback.
     */
    protected <R> R getBy(QueryCallback<R> callback) {
        return this.getJpaTemplate().execute(callback);
    }

    /**
     * Perform a bulk update.
     */
    protected int bulkUpdate(UpdateCallback callback) {
        return this.getJpaTemplate().execute(callback);
    }

// Lifecycle methods

    @Override
    public void save(T obj) {
        this.getJpaTemplate().persist(obj);
    }

    @Override
    public void delete(T obj) {
        this.getJpaTemplate().remove(obj);
    }

    @Override
    public T merge(T obj) {
        return this.getJpaTemplate().merge(obj);
    }

    @Override
    public void refresh(T obj) {
        this.getJpaTemplate().refresh(obj);
    }

    @Override
    public void detach(final Object obj) {
        this.getJpaTemplate().execute(new JpaCallback<Void>() {
            @Override
            public Void doInJpa(EntityManager entityManager) {
                entityManager.detach(obj);
                return null;
            }
        });
    }

// Session methods

    @Override
    public void flush() {
        this.getJpaTemplate().flush();
    }

    @Override
    public void setFlushMode(final FlushModeType flushMode) {
        this.getJpaTemplate().execute(new JpaCallback<Void>() {
            @Override
            public Void doInJpa(EntityManager entityManager) {
                entityManager.setFlushMode(flushMode);
                return null;
            }
        });
    }

    @Override
    public void clear() {
        this.getJpaTemplate().execute(new JpaCallback<Void>() {
            @Override
            public Void doInJpa(EntityManager entityManager) {
                entityManager.clear();
                return null;
            }
        });
    }

    @Override
    public boolean isReadOnly() {
        return TransactionSynchronizationManager.isCurrentTransactionReadOnly();
    }

// Type and cast methods

    /**
     * Cast the given object to this instance's persistent instance type.
     */
    protected T cast(Object obj) {
        return this.type.cast(obj);
    }

    /**
     * Cast the given list to a list of this instance's persistent instance type.
     * Does not actually inspect the contents of the list.
     */
    @SuppressWarnings("unchecked")
    protected List<T> castList(List<?> list) {
        return (List<T>)list;
    }

// Helper methods

    private TypedQuery<T> buildQuery(EntityManager entityManager, String queryString, Object[] params) {
        TypedQuery<T> query = entityManager.createQuery(queryString, this.type);
        if (params != null) {
            for (int i = 0; i < params.length; i++)
                query.setParameter(i + 1, params[i]);
        }
        return query;
    }

// Helper classes

    /**
     * Convenience subclass of {@link QueryCallback} for use by DAO subclasses when returning lists of persistent instances.
     */
    protected abstract class DAOQueryListCallback extends TypedQueryCallback<T, List<T>> {

        @Override
        protected final List<T> executeQuery(TypedQuery<T> query) {
            return query.getResultList();
        }
    }

    /**
     * Convenience subclass of {@link QueryCallback} for use by DAO subclasses when returning a single persistent instance.
     */
    protected abstract class DAOQueryUniqueCallback extends TypedQueryCallback<T, T> {

        @Override
        protected final T executeQuery(TypedQuery<T> query) {
            return query.getSingleResult();
        }
    }

    /**
     * Convenience subclass of {@link CriteriaCallback} for use by DAO subclasses when returning lists of persistent instances.
     */
    protected abstract class DAOCriteriaListCallback extends CriteriaCallback<T, List<T>> {

        protected DAOCriteriaListCallback() {
            super(AbstractDAO.this.type);
        }

        @Override
        protected final List<T> executeQuery(TypedQuery<T> query) {
            return query.getResultList();
        }
    }

    /**
     * Convenience subclass of {@link CriteriaCallback} for use by DAO subclasses when returning a single persistent instance.
     */
    protected abstract class DAOCriteriaUniqueCallback extends CriteriaCallback<T, T> {

        protected DAOCriteriaUniqueCallback() {
            super(AbstractDAO.this.type);
        }

        @Override
        protected final T executeQuery(TypedQuery<T> query) {
            return query.getSingleResult();
        }
    }
}

