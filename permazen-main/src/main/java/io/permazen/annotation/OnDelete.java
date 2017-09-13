
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates Permazen model class methods that are to be invoked whenever an object is about to be deleted.
 *
 * <p>
 * Notifications are delivered in the same thread that deletes the object, before the delete actually occurs.
 * At most one delete notification will ever be delivered for any object deletion event.
 *
 * <p>
 * The annotated method must be an instance method (i.e., not static), return void, and take zero parameters.
 * It may have any level of access, including {@code private}.
 *
 * <p><b>Meta-Annotations</b></p>
 *
 * <p>
 * This annotation may be configured indirectly as a Spring
 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#beans-meta-annotations">meta-annotation</a>
 * when {@code spring-core} is on the classpath.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Documented
public @interface OnDelete {

    /**
     * Determines whether this annotation should also be enabled for
     * {@linkplain io.permazen.SnapshotJTransaction snapshot transaction} objects.
     * If unset, notifications will only be delivered to non-snapshot (i.e., normal) database instances.
     *
     * @return whether enabled for snapshot transactions
     * @see io.permazen.SnapshotJTransaction
     */
    boolean snapshotTransactions() default false;
}

