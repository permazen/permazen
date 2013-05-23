
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation for {@link org.springframework.transaction.annotation.Transactional @Transactional} methods that
 * want to have transactions automatically retried when they fail due to a transient exception. A transient exception
 * is one that Spring would translate into a
 * {@link org.springframework.dao.TransientDataAccessException TransientDataAccessException}.
 * </p>
 *
 * <p>
 * This automatic retry logic is very handy for solving the problem of transient deadlocks that can occur in complex Java/ORM
 * applications. Due to the ORM layer hiding the details of the underlying data access patterns, it's often difficult
 * to design Java/ORM applications while ensuring that transient deadlocks at the database layer can't occur. Since these
 * deadlocks can often be dealt with simply by retrying the transaction, having retry logic automatically applied can
 * eliminate this problem.
 * </p>
 *
 * <p>
 * Note, beans involved in transactions should either be stateless, or be prepared to rollback any state changes on transaction
 * failure; of course, this is true whether or not transactions are automatically being retried, but adding automatic retry
 * can magnify pre-existing bugs of that nature.
 * </p>
 *
 * <p>
 * For the automatic retry logic to activate, the following conditions must be satisfied:
 * <ul>
 *  <li>
 *      The method (and/or the containing type) must be annotated with
 *      {@link org.springframework.transaction.annotation.Transactional @Transactional}
 *      and {@link RetryTransaction @RetryTransaction}
 *  </li>
 *  <li>
 *      The {@link org.springframework.transaction.annotation.Transactional @Transactional} annotation must have
 *      {@linkplain org.springframework.transaction.annotation.Transactional#propagation propagation} set to either
 *      {@link org.springframework.transaction.TransactionDefinition#PROPAGATION_REQUIRED PROPAGATION_REQUIRED} or
 *      {@link org.springframework.transaction.TransactionDefinition#PROPAGATION_REQUIRES_NEW PROPAGATION_REQUIRES_NEW}
 *      (other propagation values do not involve creating new transactions).
 *  </li>
 *  <li>
 *      In the case of
 *      {@link org.springframework.transaction.TransactionDefinition#PROPAGATION_REQUIRED PROPAGATION_REQUIRED} propagation,
 *      there must not be a transaction already open in the calling thread. In other words, the invoked method must
 *      be the one responsible for creating the transaction.
 *  </li>
 *  <li>
 *      The method's class must be woven (either at build time or runtime) using the
 *      <a href="http://www.eclipse.org/aspectj/doc/released/faq.php#compiler">AspectJ compiler</a>
 *      with the {@code RetryTransactionAspect} aspect (included in the <code>dellroad-stuff</code> JAR file).
 *  </li>
 *  <li>
 *      The {@code RetryTransactionAspect} aspect must be configured with a
 *      {@link org.springframework.dao.support.PersistenceExceptionTranslator PersistenceExceptionTranslator} appropriate for
 *      the ORM layer being used. The simplest way to do this is to include the aspect in your Spring application context,
 *      for example:
 *      <blockquote><code>
 *      &lt;bean class="org.dellroad.stuff.spring.RetryTransactionAspect" factory-method="aspectOf"
 *        p:exceptionTranslator-ref="myJpaDialect"/&gt;
 *       </code></blockquote>
 *  </li>
 * </ul>
 *
 * <p>
 * Normal activity is logged at trace level, retries are logged at debug level, and errors are logged at error level.
 * </p>
 * </p>
 *
 * @see org.springframework.transaction.annotation.Transactional
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface RetryTransaction {

    /**
     * Default maximum number of retry attempts.
     */
    int DEFAULT_MAX_RETRIES = 4;

    /**
     * Default initial delay.
     */
    long DEFAULT_INITIAL_DELAY = 100;

    /**
     * Default maximum delay.
     */
    long DEFAULT_MAXIMUM_DELAY = 30 * 1000;

    /**
     * The maximum number of transaction retry attempts.
     * If the transaction fails, it will be retried at most this many times.
     */
    int maxRetries() default DEFAULT_MAX_RETRIES;

    /**
     * The initial delay between retry attempts in milliseconds.
     * After the first transaction failure, we will pause for approximately this many milliseconds.
     * For additional failures we apply a randomized exponential back-off, up to a maximum of {@link #maximumDelay}.
     */
    long initialDelay() default DEFAULT_INITIAL_DELAY;

    /**
     * The maximum delay between retry attempts in milliseconds.
     * After the first transaction failure, we will pause for approximately {@link #initialDelay} milliseconds.
     * For additional failures we apply a randomized exponential back-off, up to a maximum of this value.
     */
    long maximumDelay() default DEFAULT_MAXIMUM_DELAY;
}

