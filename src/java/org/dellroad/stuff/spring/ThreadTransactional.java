
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

import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;

/**
 * Works just like Spring's {@link org.springframework.transaction.annotation.Transactional @Transactional} annotation,
 * but instead of using a fixed {@link org.springframework.transaction.PlatformTransactionManager} stored in a static
 * variable and determined once at startup, this annotation uses a
 * {@link org.springframework.transaction.PlatformTransactionManager} found dynamically at runtime in the application context
 * that is associated with the current thread by the {@link ThreadLocalContext} singleton. This allows the same transactional
 * class to be instantiated in different application contexts with different transaction managers.
 *
 * <p>
 * Stated another way, {@link ThreadConfigurable @ThreadConfigurable} does for
 * {@link org.springframework.transaction.annotation.Transactional @Transactional}
 * what {@link ThreadConfigurable @ThreadConfigurable} does for
 * {@link org.springframework.beans.factory.annotation.Configurable @Configurable}.
 * See {@link ThreadConfigurable @ThreadConfigurable} for further discussion.
 * </p>
 *
 * <p>
 * This annotation does not require or use the <code>&lt;tx:annotation-driven/&gt;</code> XML declaration.
 * As with {@link org.springframework.transaction.annotation.Transactional @Transactional}, if you need to
 * specifiy a specific {@link org.springframework.transaction.PlatformTransactionManager} by name,
 * you can do that by setting the annotation {@link #value}.
 * </p>
 *
 * <p>
 * If a {@link ThreadTransactional @ThreadTransactional}-annotated method is invoked and no application context
 * has been configured for the current thread, and there is no default set by way of the {@link ThreadLocalContext}
 * singleton, then an {@link IllegalStateException} is thrown.
 * </p>
 *
 * <p>
 * Running the AspectJ compiler on your annotated classes is required to activate this annotation.
 * </p>
 *
 * @see ThreadTransactional
 * @see ThreadLocalContext
 * @see org.springframework.transaction.annotation.Transactional
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface ThreadTransactional {

    /**
     * Transaction manager bean name.
     *
     * @see org.springframework.transaction.annotation.Transactional#value
     */
    String value() default "";

    /**
     * Transaction propagation type.
     *
     * @see org.springframework.transaction.annotation.Transactional#propagation
     */
    Propagation propagation() default Propagation.REQUIRED;

    /**
     * Transaction isolation level.
     *
     * @see org.springframework.transaction.annotation.Transactional#isolation
     */
    Isolation isolation() default Isolation.DEFAULT;

    /**
     * Timeout for this transaction.
     *
     * @see org.springframework.transaction.annotation.Transactional#timeout
     */
    int timeout() default TransactionDefinition.TIMEOUT_DEFAULT;

    /**
     * Whether the transaction is read-only.
     *
     * @see org.springframework.transaction.annotation.Transactional#readOnly
     */
    boolean readOnly() default false;

    /**
     * Rollback classes.
     *
     * @see org.springframework.transaction.annotation.Transactional#rollbackFor
     */
    Class<? extends Throwable>[] rollbackFor() default {};

    /**
     * Rollback classes.
     *
     * @see org.springframework.transaction.annotation.Transactional#rollbackForClassName
     */
    String[] rollbackForClassName() default {};

    /**
     * Non-rollback classes.
     *
     * @see org.springframework.transaction.annotation.Transactional#noRollbackFor
     */
    Class<? extends Throwable>[] noRollbackFor() default {};

    /**
     * Non-rollback classes.
     *
     * @see org.springframework.transaction.annotation.Transactional#noRollbackForClassName
     */
    String[] noRollbackForClassName() default {};

}
