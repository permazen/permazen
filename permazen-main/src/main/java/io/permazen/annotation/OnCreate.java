
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.DetachedPermazenTransaction;
import io.permazen.PermazenObject;
import io.permazen.PermazenTransaction;
import io.permazen.core.Transaction;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates Permazen model class methods that are to be invoked whenever a database object is newly created.
 *
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/prism.min.js"></script>
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/components/prism-java.min.js"></script>
 * <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/themes/prism.min.css" rel="stylesheet"/>
 *
 * <p>
 * Note that there is a subtle distinction between (a) the creation of database objects in the database, and
 * (b) the instantiation of Java model objects that represent database objects (i.e., {@link PermazenObject}s).
 * These two events do not occur at the same time; in particular, distinct Java model objects are instantiated to
 * represent the same database object in different transactions. In addition, it's even possible for a Java model
 * object to be instantiated when no corresponding database object exists in the database, e.g., via
 * {@link PermazenTransaction#get(io.permazen.core.ObjId)}.
 *
 * <p>
 * Methods that are annotated with {@link OnCreate &#64;OnCreate} are invoked only for events of type (a).
 * As a consequence, for any database fields that require default initialization, this initialization should be
 * performed not in a Java constructor but rather in an {@link OnCreate &#64;OnCreate}-annotated method.
 *
 * <p>
 * For example, instead of this:
 * <pre><code class="language-java">
 *  &#64;PermazenType
 *  public abstract class Event {
 *
 *      protected Event() {
 *          this.setCreateTime(new Date());
 *      }
 *
 *      &#64;NotNull
 *      public abstract Date getCreateTime();
 *      public abstract void setCreateTime(Date createTime);
 *
 *      ...
 * </code></pre>
 *
 * <p>
 * do this:
 * <pre><code class="language-java">
 *  &#64;PermazenType
 *  public abstract class Event {
 *
 *      &#64;OnCreate
 *      private void initializeCreateTime() {
 *          this.setCreateTime(new Date());
 *      }
 *
 *      &#64;NotNull
 *      public abstract Date getCreateTime();
 *      public abstract void setCreateTime(Date createTime);
 *
 *      ...
 * </code></pre>
 *
 * <p>
 * Notifications are delivered in the same thread that created the object, immediately after the object is created.
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
 * <a href="https://docs.spring.io/spring-framework/reference/core/beans/classpath-scanning.html#beans-meta-annotations">meta-annotation</a>
 * when {@code spring-core} is on the classpath.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Documented
public @interface OnCreate {
}
