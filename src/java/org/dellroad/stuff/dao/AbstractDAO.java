
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.dao;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.FlushModeType;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;

import org.dellroad.stuff.spring.AbstractBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Support superclass for JPA DAO implementations.
 *
 * @param <T> persistent instance type
 * @see org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor
 */
public abstract class AbstractDAO<T> extends AbstractBean implements DAO<T> {

    /**
     * Persistent instance type.
     */
    protected final Class<T> type;

    /**
     * The configured {@link EntityManager}.
     */
    private EntityManager entityManager;

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

// Configuration and lifecycle methods

    /**
     * Get the configured {@link EntityManager}.
     */
    protected EntityManager getEntityManager() {
        return this.entityManager;
    }

    /**
     * Set the configured {@link EntityManager}.
     *
     * <p>
     * This method has a {@link PersistenceContext @PersistenceContext} annotation and normally
     * would be invoked automatically by Spring.
     */
    @PersistenceContext
    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (this.getEntityManager() == null)
            throw new IllegalArgumentException("no entityManager configured");
    }

// Meta-data methods

    @Override
    public Class<T> getType() {
        return this.type;
    }

// Access methods

    @Override
    public T getById(long id) {
        return this.getEntityManager().find(this.type, id);
    }

    @Override
    public List<T> getAll() {
        return this.getBy(new DAOCriteriaListCallback() {
            @Override
            protected void configureQuery(CriteriaQuery<T> criteriaQuery, CriteriaBuilder criteriaBuilder) {
                criteriaQuery.from(this.type);
            }
        });
    }

    @Override
    public T getReference(long id) {
        return this.getEntityManager().getReference(this.type, id);
    }

    /**
     * Find instances using a query string and query parameters.
     */
    protected List<T> find(final String queryString, final Object... params) {
        return this.getBy(new DAOQueryListCallback() {
            @Override
            protected TypedQuery<T> buildQuery(EntityManager em) {
                return AbstractDAO.this.buildQuery(em, queryString, params);
            }
        });
    }

    /**
     * Find a unique instance using a query string and query parameters.
     *
     * @return unique instance found, or null if none was found
     */
    protected T findUnique(final String queryString, final Object... params) {
        return this.getBy(new DAOQueryUniqueCallback() {
            @Override
            protected TypedQuery<T> buildQuery(EntityManager em) {
                return AbstractDAO.this.buildQuery(em, queryString, params);
            }
        });
    }

    /**
     * Search using a {@link QueryCallback}.
     */
    protected <R> R getBy(QueryCallback<R> callback) {
        return callback.query(this.getEntityManager());
    }

    /**
     * Perform a bulk update.
     */
    protected int bulkUpdate(UpdateCallback callback) {
        return callback.query(this.getEntityManager());
    }

// Lifecycle methods

    @Override
    public void save(T obj) {
        this.getEntityManager().persist(obj);
    }

    @Override
    public void delete(T obj) {
        this.getEntityManager().remove(obj);
    }

    @Override
    public T merge(T obj) {
        return this.getEntityManager().merge(obj);
    }

    @Override
    public void refresh(T obj) {
        this.getEntityManager().refresh(obj);
    }

    @Override
    public void detach(Object obj) {
        this.getEntityManager().detach(obj);
    }

// Session methods

    @Override
    public void flush() {
        this.getEntityManager().flush();
    }

    @Override
    public void setFlushMode(FlushModeType flushMode) {
        this.getEntityManager().setFlushMode(flushMode);
    }

    @Override
    public void clear() {
        this.getEntityManager().clear();
    }

    @Override
    public boolean isReadOnly() {
        return TransactionSynchronizationManager.isCurrentTransactionReadOnly();
    }

    @Override
    public boolean contains(T obj) {
        return this.getEntityManager().contains(obj);
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

    private TypedQuery<T> buildQuery(EntityManager em, String queryString, Object[] params) {
        TypedQuery<T> query = em.createQuery(queryString, this.type);
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
     *
     * <p>
     * Returns null if instance is not found.
     */
    protected abstract class DAOQueryUniqueCallback extends TypedQueryCallback<T, T> {

        @Override
        protected final T executeQuery(TypedQuery<T> query) {
            try {
                return query.getSingleResult();
            } catch (NoResultException e) {
                return null;
            } catch (EmptyResultDataAccessException e) {
                return null;
            }
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
     *
     * <p>
     * Returns null if instance is not found.
     */
    protected abstract class DAOCriteriaUniqueCallback extends CriteriaCallback<T, T> {

        protected DAOCriteriaUniqueCallback() {
            super(AbstractDAO.this.type);
        }

        @Override
        protected final T executeQuery(TypedQuery<T> query) {
            try {
                return query.getSingleResult();
            } catch (NoResultException e) {
                return null;
            } catch (EmptyResultDataAccessException e) {
                return null;
            }
        }
    }
}

