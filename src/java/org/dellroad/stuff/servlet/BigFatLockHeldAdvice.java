
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.servlet;

import java.lang.reflect.Method;

import org.springframework.aop.aspectj.AspectInstanceFactory;
import org.springframework.aop.aspectj.AspectJExpressionPointcut;
import org.springframework.aop.aspectj.AspectJMethodBeforeAdvice;

/**
 * AspectJ method "before advice" that verifies that the {@link BigFatLock} is held.
 */
public class BigFatLockHeldAdvice extends AspectJMethodBeforeAdvice {

    /**
     * Constructor.
     */
    public BigFatLockHeldAdvice(Method aspectJBeforeAdviceMethod, AspectJExpressionPointcut pointcut, AspectInstanceFactory aif) {
        super(aspectJBeforeAdviceMethod, pointcut, aif);
    }

    /**
     * Verifies the {@link BigFatLock} is held. If not, {@link #lockNotHeld} is invoked.
     */
    @Override
    public void before(Method method, Object[] args, Object target) {
        if (!BigFatLock.isLockHeld())
            this.lockNotHeld();
    }

    /**
     * Invoked in cases where the current thread does not hold the {@link BigFatLock}.
     *
     * <p>
     * The implementation in {@link BigFatLockHeldAdvice} throws a {@link BigFatLockNotHeldException}.
     * Subclasses can override to suit.
     */
    protected void lockNotHeld() {
        throw new BigFatLockNotHeldException();
    }
}

