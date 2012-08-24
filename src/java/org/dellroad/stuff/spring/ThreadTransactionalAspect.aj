
/*
 * Copyright (C) 2011 Archie L. Cobbs and other authors. All rights reserved.
 *
 * $Id: ThreadTransactionalAspect.aj 282 2012-02-16 20:48:06Z archie.cobbs $
 */

package org.dellroad.stuff.spring;

import java.io.Serializable;
import java.lang.reflect.Method;

import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.aspectj.AbstractTransactionAspect;
import org.springframework.transaction.interceptor.AbstractFallbackTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.interceptor.TransactionAspectUtils;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.util.StringUtils;

/**
 * Works just like Spring's {@link Transactional @Transactional} annotation, but instead of using a fixed
 * {@link PlatformTransactionManager} stored in a static variable and determined once at startup, uses a
 * {@link PlatformTransactionManager} found dynamically at runtime in the application context associated
 * with the current thread by the {@link ThreadLocalContext} singleton.
 *
 * <p>
 * This implementation is derived from Spring's {@code org.springframework.transaction.aspectj.AnnotationTransactionAspect}
 * implementation and therefore shares its license (also Apache).
 * </p>
 *
 * @see ThreadTransactional
 */
public aspect ThreadTransactionalAspect extends AbstractTransactionAspect {

    public ThreadTransactionalAspect() {
        super(new ThreadTransactionalTransactionAttributeSource());
    }

// Pointcuts

    private pointcut executionOfAnyPublicMethodInAtThreadTransactionalType() :
        execution(public * ((@ThreadTransactional *)+).*(..)) && within(@ThreadTransactional *);

    private pointcut executionOfThreadTransactionalMethod() :
        execution(@ThreadTransactional * *(..));

    protected pointcut transactionalMethodExecution(Object txObject) :
        (executionOfAnyPublicMethodInAtThreadTransactionalType() || executionOfThreadTransactionalMethod()) && this(txObject);

// InitializingBean

    @Override
    public void afterPropertiesSet() {
        // Since we are going to find the PlatformTransactionManager in a context that is determined dynamically each time
        // we are invoked instead of at configuration time, we don't require that the context is known ahead of time
        // nor do we use the superclass fields. So skip the checks to avoid the IllegalStateException.
    }

// Override this method in TransactionAspectSupport because it uses instance fields

    @Override
    public PlatformTransactionManager determineTransactionManager(TransactionAttribute transactionAttribute) {

        // Get ThreadLocalContext singleton
        ThreadLocalContext threadLocalContext = ThreadLocalContext.getInstance();
        if (threadLocalContext == null) {
            throw new IllegalStateException("can't determine transaction manager for @" + ThreadTransactional.class.getSimpleName()
              + "-annotated bean because the ThreadLocalContext singleton instance has been set to null");
        }

        // Get ConfigurableApplicationContext
        ConfigurableApplicationContext context = threadLocalContext.get();
        if (context == null) {
            throw new IllegalStateException("can't determine transaction manager for @" + ThreadTransactional.class.getSimpleName()
              + "-annotated bean because no ConfigurableApplicationContext has been configured for the current thread via "
              + ThreadLocalContext.class.getName() + ".set() nor has a default been set");
        }

        // Get the PlatformTransactionManager, by name or by type
        String qualifier = transactionAttribute.getQualifier();
        if (StringUtils.hasLength(qualifier))
            return TransactionAspectUtils.getTransactionManager(context.getBeanFactory(), qualifier);
        return BeanFactoryUtils.beanOfTypeIncludingAncestors(context.getBeanFactory(), PlatformTransactionManager.class);
    }

// TransactionAttributeSource wrapper that looks for @ThreadTransactional annotations

    @SuppressWarnings("serial")
    private static class ThreadTransactionalTransactionAttributeSource extends AbstractFallbackTransactionAttributeSource
      implements Serializable {

        @Override
        protected TransactionAttribute findTransactionAttribute(Method method) {
            return new ThreadTransactionalAnnotationParser().parseTransactionAnnotation(method);
        }

        @Override
        protected TransactionAttribute findTransactionAttribute(Class<?> clazz) {
            return new ThreadTransactionalAnnotationParser().parseTransactionAnnotation(clazz);
        }
    }
}

