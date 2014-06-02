
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
 * to design Java/ORM applications such that transient deadlocks at the database layer can't occur. Since these
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
 * The {@link RetryTransaction @RetryTransaction} annotation is ignored unless all of the following conditions are satisfied:
 * <ul>
 *  <li>
 *      The method (and/or the containing type) must be annotated with both
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
 *      <pre>
 *
 *      &lt;bean class="org.dellroad.stuff.spring.RetryTransactionAspect" factory-method="aspectOf"
 *        p:persistenceExceptionTranslator-ref="myJpaDialect"/&gt;
 *      </pre>This also gives you the opportunity to change the default values for {@link #maxRetries}, {@link #initialDelay},
 *      and {@link #maximumDelay}, which are applied when not explicitly overridden in the annotation, for example:
 *      <pre>
 *
 *      &lt;bean class="org.dellroad.stuff.spring.RetryTransactionAspect" factory-method="aspectOf"
 *        p:persistenceExceptionTranslator-ref="myJpaDialect" p:maxRetriesDefault="2"
 *        p:initialDelayDefault="25" p:maximumDelayDefault="5000"/&gt;
 *      </pre>
 *  </li>
 * </ul>
 *
 * <p>
 * Logging behavior: Normal activity is logged at trace level, retries are logged at debug level, and errors are logged
 * at error level.
 * </p>
 *
 * <p>
 * Transactional code can determine the transaction attempt number using the {@link RetryTransactionProvider} interface
 * implemented by the aspect. {@link RetryTransactionProvider#getAttemptNumber} method returns the current attempt number
 * (1, 2, 3...), or zero if the current thread is not executing within activated retry logic:
 *  <blockquote><pre>
 *      import org.dellroad.stuff.spring.RetryTransactionProvider;
 *      ...
 *
 *      &#64;Autowired
 *      private RetryTransactionProvider retryTransactionProvider;
 *      ...
 *
 *      &#64;RetryTransaction
 *      &#64;Transactional
 *      public void doSomething() {
 *          ...
 *          final int attempt = this.retryTransactionProvider.getAttempt();
 *          ...
 *      }
 *   </pre></blockquote>
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
     * Default {@linkplain #maxRetries maximum number of retry attempts}, used when the {@link #maxRetries maxRetries}
     * value is not explicitly set in an instance of this annotation.
     * This default value can be overridden by configuring the {@code maxRetriesDefault} property on the aspect itself.
     */
    int DEFAULT_MAX_RETRIES = 4;

    /**
     * Default {@linkplain #initialDelay initial delay}, in milliseconds, used when the {@link #initialDelay initialDelay}
     * value is not explicitly set in an instance of this annotation.
     * This default value can be overridden by configuring the {@code initialDelayDefault} property on the aspect itself.
     */
    long DEFAULT_INITIAL_DELAY = 100;

    /**
     * Default {@linkplain #maximumDelay maximum delay}, in milliseconds, used when the {@link #maximumDelay maximumDelay}
     * value is not explicitly set in an instance of this annotation.
     * This default value can be overridden by configuring the {@code maximumDelayDefault} property on the aspect itself.
     */
    long DEFAULT_MAXIMUM_DELAY = 30 * 1000;

    /**
     * The maximum number of transaction retry attempts.
     *
     * <p>
     * If the transaction fails, it will be retried at most this many times.
     * This limit applies to retries only; it does not apply to the very first attempt, which is always made.
     * So a value of zero means at most one attempt.
     * </p>
     *
     * <p>
     * If this property is not set explicitly, the default value of {@code -1} indicates that the aspect-wide default value
     * ({@value #DEFAULT_MAX_RETRIES} by default), should be used.
     * </p>
     */
    int maxRetries() default -1;

    /**
     * The initial delay between retry attempts in milliseconds.
     * After the first transaction failure, we will pause for approximately this many milliseconds.
     * For additional failures we apply a randomized exponential back-off, up to a maximum of {@link #maximumDelay}.
     *
     * <p>
     * If this property is not set explicitly, the default value of {@code -1} indicates that the aspect-wide default value
     * ({@value #DEFAULT_INITIAL_DELAY} milliseconds by default), should be used.
     * </p>
     */
    long initialDelay() default -1;

    /**
     * The maximum delay between retry attempts in milliseconds.
     * After the first transaction failure, we will pause for approximately {@link #initialDelay} milliseconds.
     * For additional failures we apply a randomized exponential back-off, up to a maximum of this value.
     *
     * <p>
     * If this property is not set explicitly, the default value of {@code -1} indicates that the aspect-wide default value
     * ({@value #DEFAULT_MAXIMUM_DELAY} milliseconds by default), should be used.
     * </p>
     */
    long maximumDelay() default -1;
}

