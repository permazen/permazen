
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.aspectj.lang.reflect.MethodSignature;
import org.dellroad.stuff.spring.AbstractBean;
import org.dellroad.stuff.spring.RetryTransactionAspect;
import org.springframework.core.annotation.AnnotationUtils;

/**
 * Implements {@link PersistentObjectTransactional} semantics.
 *
 * @see PersistentObjectTransactional
 */
public aspect PersistentObjectTransactionalAspect extends AbstractBean {

    private List<PersistentObjectTransactionManager<?>> managerList;
    private HashMap<String, PersistentObjectTransactionManager<?>> managerMap;

    /**
     * Configure a single {@link PersistentObjectTransactionManager} associated with this aspect.
     */
    public void setPersistentObjectTransactionManager(PersistentObjectTransactionManager<?> manager) {
        if (this.managerList == null)
            this.managerList = new ArrayList<PersistentObjectTransactionManager<?>>(1);
        this.managerList.add(manager);
    }

    /**
     * Configure a list of {@link PersistentObjectTransactionManager}s to be associated with this aspect.
     */
    public List<PersistentObjectTransactionManager<?>> getPersistentObjectTransactionManagers() {
        return this.managerList;
    }
    public void setPersistentObjectTransactionManagers(List<PersistentObjectTransactionManager<?>> managerList) {
        this.managerList = managerList;
    }

// InitializingBean

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (this.managerList == null || this.managerList.isEmpty())
            throw new IllegalArgumentException("no PersistentObjectTransactionManagers have been configured");
        this.managerMap = new HashMap<String, PersistentObjectTransactionManager<?>>(this.managerList.size());
        for (PersistentObjectTransactionManager<?> manager : this.managerList)
            this.managerMap.put(manager.getName(), manager);
    }

// Aspect

    // Ensure that this aspect is woven outside of the RetryTransactionAspect
    declare precedence : PersistentObjectTransactionalAspect, RetryTransactionAspect;

    Object around() : executionOfPersistentObjectTransactionalMethod() {

        // Sanity check we are configured
        if (this.managerMap == null) {
            throw new RuntimeException("the @PersistentObjectTransactional aspect must be configured with"
               + " one or more PersistentObjectTransactionManagers before use");
        }

        // Get method info
        final MethodSignature methodSignature = (MethodSignature)thisJoinPoint.getSignature();
        final Method method = methodSignature.getMethod();

        // Get @PersistentObjectTransactional annotation
        final PersistentObjectTransactional annotation = AnnotationUtils.getAnnotation(method, PersistentObjectTransactional.class);
        if (annotation == null)
            throw new RuntimeException("internal error: no @PersistentObjectTransactional annotation found for method " + method);
        final String managerName = annotation.managerName();
        final boolean readOnly = annotation.readOnly();
        final boolean shared = annotation.shared();
        final boolean async = annotation.async();

        // Find the right PersistentObjectTransactionManager
        final PersistentObjectTransactionManager<?> manager = managerName.equals("") && this.managerMap.size() == 1 ?
          this.managerMap.values().iterator().next() : this.managerMap.get(managerName);
        if (manager == null) {
            if (!managerName.equals("")) {
                throw new RuntimeException("@PersistentObjectTransactional: no PersistentObjectTransactionManager named `"
                  + managerName + "' has been configured with this aspect");
            } else {
                throw new RuntimeException("@PersistentObjectTransactional: multiple PersistentObjectTransactionManagers"
                  + " have been configured with this aspect, but the `managerName' annotation property was not set");
            }
        }

        // Create callback
        final Throwable stackTrace = async ? new Throwable() : null;
        final Callable<Object> callback = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    return proceed();
                } catch (Exception e) {
                    if (async) {
                        manager.prependStackFrames(e, stackTrace, "Asynchronous PersistentObjectTransactionManager Transaction", 0);
                        PersistentObjectTransactionalAspect.this.log.error(
                          "error during asynchronous @PersistentObjectTransactional transaction: " + e, e);
                    }
                    throw e;
                }
            }
        };

        // Execute transaction, either synchronously or asynchronously
        if (async) {
            manager.scheduleTransaction(callback, readOnly, shared);
            return null;
        } else {
            try {
                return manager.performTransaction(callback, readOnly, shared);
            } catch (Error e) {
                throw e;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw manager.<RuntimeException>maskException(e);                           // re-throw checked exception
            }
        }
    }

    /**
     * Matches the execution of any method with the {@link PersistentObjectTransactional &#64;PersistentObjectTransactional}
     * annotation.
     */
    private pointcut executionOfPersistentObjectTransactionalMethod() : execution(@PersistentObjectTransactional * *(..));
}

