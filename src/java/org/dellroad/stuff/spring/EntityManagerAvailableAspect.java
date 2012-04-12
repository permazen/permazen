
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.orm.jpa.EntityManagerFactoryUtils;
import org.springframework.orm.jpa.EntityManagerHolder;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Implements the {@link EntityManagerAvailable @EntityManagerAvailable} annotation <code>@AspectJ</code> advice.
 * The {@link #setEntityManagerFactory entityManagerFactory} property is required.
 *
 * @see EntityManagerAvailable
 */
@Aspect
public class EntityManagerAvailableAspect implements InitializingBean {

    private EntityManagerFactory entityManagerFactory;

    /**
     * Configure the {@link EntityManagerFactory}.
     * Required property.
     */
    public void setEntityManagerFactory(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.entityManagerFactory == null)
            throw new Exception("entityManagerFactory property not configured");
    }

    @Around(value = "@annotation(annotation)", argNames = "proceedingJoinPoint, annotation")
    public Object ensureEntityManagerAvailable(ProceedingJoinPoint proceedingJoinPoint, EntityManagerAvailable annotation)
      throws Throwable {

        // Get propagation
        final Propagation propagation = annotation.value();

        // Is there an existing EntityManager?
        EntityManagerHolder priorEntityManagerHolder = (EntityManagerHolder)TransactionSynchronizationManager.getResource(
          this.entityManagerFactory);

        // Check propagation for what to do
        boolean createNew = false;
        boolean suspendOld = false;
        switch (propagation) {
        case MANDATORY:
            if (priorEntityManagerHolder == null)
                throw new IllegalTransactionStateException("propagation is " + propagation + " but no EnityManager exists");
            break;
        case NESTED:
            createNew = true;
            break;
        case NEVER:
            if (priorEntityManagerHolder != null)
                throw new IllegalTransactionStateException("propagation is " + propagation + " but EnityManager exists");
            break;
        case NOT_SUPPORTED:
            if (priorEntityManagerHolder != null)
                suspendOld = true;
            break;
        case REQUIRED:
            if (priorEntityManagerHolder == null)
                createNew = true;
            break;
        case REQUIRES_NEW:
            if (priorEntityManagerHolder != null)
                suspendOld = true;
            createNew = true;
            break;
        case SUPPORTS:
            break;
        default:
            throw new RuntimeException("unsupported propagation value: " + propagation);
        }

        // Unbind old EntityManager if necessary
        if (suspendOld)
            TransactionSynchronizationManager.unbindResource(this.entityManagerFactory);
        try {

            // Create new EntityManager if necessary
            if (createNew) {
                EntityManager entityManager;
                try {
                    entityManager = this.entityManagerFactory.createEntityManager();
                } catch (PersistenceException e) {
                    throw new DataAccessResourceFailureException("failed to create JPA EntityManager", e);
                }
                TransactionSynchronizationManager.bindResource(this.entityManagerFactory, new EntityManagerHolder(entityManager));
            }
            try {

                // Invoke method
                return proceedingJoinPoint.proceed();
            } finally {

                // Unbind new EntityManager that we created
                if (createNew) {
                    EntityManagerHolder entityManagerHolder = (EntityManagerHolder)TransactionSynchronizationManager.unbindResource(
                      this.entityManagerFactory);
                    EntityManagerFactoryUtils.closeEntityManager(entityManagerHolder.getEntityManager());
                }
            }
        } finally {

            // Re-bind prior EntityManager if needed
            if (suspendOld) {
                assert priorEntityManagerHolder != null;
                TransactionSynchronizationManager.bindResource(this.entityManagerFactory, priorEntityManagerHolder);
            }
        }
    }
}

