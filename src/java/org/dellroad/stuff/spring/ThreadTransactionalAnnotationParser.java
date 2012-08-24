
/*
 * Copyright (C) 2011 Archie L. Cobbs and other authors. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import java.lang.reflect.AnnotatedElement;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.transaction.annotation.SpringTransactionAnnotationParser;
import org.springframework.transaction.interceptor.TransactionAttribute;

/**
 * Strategy implementation for parsing the {@link ThreadTransactional @ThreadTransactional} annotation.
 * Works just like {@link SpringTransactionAnnotationParser} but parses {@link ThreadTransactional @ThreadTransactional}
 * annotations instead.
 *
 * @see ThreadTransactional
 */
@SuppressWarnings("serial")
public class ThreadTransactionalAnnotationParser extends SpringTransactionAnnotationParser {

    @Override
    public TransactionAttribute parseTransactionAnnotation(AnnotatedElement annotatedElement) {
        ThreadTransactional threadTransactional = AnnotationUtils.getAnnotation(annotatedElement, ThreadTransactional.class);
        if (threadTransactional == null)
            return null;
        return this.parseTransactionAnnotation(new ThreadTransactionalAdapter(threadTransactional));
    }
}

