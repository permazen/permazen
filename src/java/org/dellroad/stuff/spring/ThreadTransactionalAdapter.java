
/*
 * Copyright (C) 2011 Archie L. Cobbs and other authors. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import java.lang.annotation.Annotation;

import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Adapter that converts a {@link ThreadTransactional @ThreadTransactional} annotation into a
 * {@link Transactional adTransactional} one.
 *
 * <p>
 * Used internally by {@link ThreadTransactionalAnnotationParser}.
 *
 * @see ThreadTransactional
 * @see Transactional
 */
public class ThreadTransactionalAdapter implements Annotation, Transactional {

    private final ThreadTransactional threadTransactional;

    public ThreadTransactionalAdapter(ThreadTransactional threadTransactional) {
        if (threadTransactional == null)
            throw new IllegalArgumentException("null threadTransactional");
        this.threadTransactional = threadTransactional;
    }

    @Override
    public Class<? extends Annotation> annotationType() {
        return Transactional.class;
    }

    @Override
    public String value() {
        return this.threadTransactional.value();
    }

    @Override
    public Propagation propagation() {
        return this.threadTransactional.propagation();
    }

    @Override
    public Isolation isolation() {
        return this.threadTransactional.isolation();
    }

    @Override
    public int timeout() {
        return this.threadTransactional.timeout();
    }

    @Override
    public boolean readOnly() {
        return this.threadTransactional.readOnly();
    }

    @Override
    public Class<? extends Throwable>[] rollbackFor() {
        return this.threadTransactional.rollbackFor();
    }

    @Override
    public String[] rollbackForClassName() {
        return this.threadTransactional.rollbackForClassName();
    }

    @Override
    public Class<? extends Throwable>[] noRollbackFor() {
        return this.threadTransactional.noRollbackFor();
    }

    @Override
    public String[] noRollbackForClassName() {
        return this.threadTransactional.noRollbackForClassName();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() == this.getClass())
            return false;
        ThreadTransactionalAdapter that = (ThreadTransactionalAdapter)obj;
        return this.threadTransactional.equals(that.threadTransactional);
    }

    @Override
    public int hashCode() {
        return this.threadTransactional.hashCode();
    }
}

