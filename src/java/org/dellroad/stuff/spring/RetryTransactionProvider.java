
/*
 * Copyright (C) 2011 Archie L. Cobbs and other authors. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.springframework.dao.support.PersistenceExceptionTranslator;

/**
 * Interface implemented by the {@code RetryTransactionAspect}, which implements the {@link RetryTransaction} functionality.
 *
 * @see RetryTransaction
 */
public interface RetryTransactionProvider {

    /**
     * Get the configured exception translator.
     */
    PersistenceExceptionTranslator getPersistenceExceptionTranslator();

    /**
     * Get the aspect-wide default for {@link RetryTransaction#maxRetries}.
     */
    int getMaxRetriesDefault();

    /**
     * Get the aspect-wide default for {@link RetryTransaction#initialDelay}.
     */
    long getInitialDelayDefault();

    /**
     * Get the aspect-wide default for {@link RetryTransaction#maximumDelay}.
     */
    long getMaximumDelayDefault();

    /**
     * Get the current transaction attempt number.
     *
     * @return transaction attempt number, or zero if the aspect is not active in the current thread
     */
    int getAttemptNumber();
}

