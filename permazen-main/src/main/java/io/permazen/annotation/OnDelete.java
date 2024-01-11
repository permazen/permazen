
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.DetachedPermazenTransaction;
import io.permazen.core.Transaction;

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
 * <p>
 * Some notifications may need to be ignored by objects in {@linkplain DetachedPermazenTransaction detached} transactions;
 * you can use {@code this.isDetached()} to detect that situation.
 *
 * <p>
 * Actions that have effects visible to the outside world should be made contingent on successful transaction commit,
 * for example, via {@link Transaction#addCallback Transaction.addCallback()}.
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
}
