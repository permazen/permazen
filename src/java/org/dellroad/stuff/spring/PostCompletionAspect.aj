
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import java.util.concurrent.Executor;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.SuppressAjWarnings;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.annotation.Order;
import org.springframework.transaction.aspectj.AnnotationTransactionAspect;

/**
 * Implements the {@link PostCompletionSupport @PostCompletionSupport} annotation AOP aspect.
 * The {@link #setExecutor executor} property is required.
 *
 * <p>
 * See {@link PostCompletionSupport @PostCompletionSupport} for an example of how this class is normally used.
 *
 * @see PostCompletionSupport
 */
public aspect PostCompletionAspect implements InitializingBean, DisposableBean {

    declare precedence : PostCompletionAspect, AnnotationTransactionAspect;

    private Executor executor;

    /**
     * Configure the {@link Executor} which will be used to execute the post-completion actions.
     */
    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.executor == null)
            throw new Exception("no executor configured");
    }

    @Override
    public void destroy() {
        this.executor = null;
    }

    private pointcut executionOfPostCompletionSupportMethod() :
      execution(@PostCompletionSupport * *(..));

    @SuppressAjWarnings("adviceDidNotMatch")
    before() : executionOfPostCompletionSupportMethod() {
        PostCompletion.push();
    }

    @SuppressAjWarnings("adviceDidNotMatch")
    after() returning() : executionOfPostCompletionSupportMethod() {
        PostCompletionRegistry registry = PostCompletion.get();
        if (!PostCompletion.pop())
            registry.execute(this.executor, true);
    }

    @SuppressAjWarnings("adviceDidNotMatch")
    after() throwing(Throwable t) : executionOfPostCompletionSupportMethod() {
        PostCompletionRegistry registry = PostCompletion.get();
        if (!PostCompletion.pop())
            registry.execute(this.executor, false);
    }
}

