
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.DetachedPermazenTransaction;
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
 * <p><b>Overview</b></p>
 *
 * <p>
 * When a <i>matching object</i> is created, annotated methods are invoked just after the object's creation.
 *
 * <p>
 * For instance methods, the annotated method may take either zero or one parameter (the newly created object).
 * Zero is typical, in which case a matching object is any object that is an instance of the method's declaring class,
 * i.e., newly created objects receive their own notifications.
 *
 * <p>
 * If the method has one parameter, then newly created objects still notify themselves but only when their types
 * match the parameter's type (the new object is both the method receiver and method parameter). The latter case
 * is less common but useful for example when a superclass method is only interested in the creation of
 * specific sub-types.
 *
 * <p>
 * For static methods, a method parameter is required and a matching object is one whose type is compatible with it.
 *
 * <p>
 * The annotated method may may have any level of access, including {@code private}.
 *
 * <p>
 * A class may have multiple {@link OnCreate &#64;OnCreate} methods, each with a specific purpose.
 *
 * <p>
 * Note that there is a subtle distinction between (a) the creation of objects in the database (i.e., the event that
 * this annotation concerns), and (b) the instantiation of an actual Java model object representing a database object.
 * These two events are different; in particular, distinct Java objects are created to represent the same database
 * object in different transactions. It's even possible for a Java model object to be instantiated even though no
 * corresponding database object exists in the database (via
 * {@link PermazenTransaction#get(io.permazen.core.ObjId) PermazenTransaction.get()}).
 *
 * <p>
 * As a consequence, any operations specific to the creation of new database instance, such as one-time initialization
 * of database fields, should take place in {@link OnCreate &#64;OnCreate}-annotated methods instead of constructors.
 *
 * <p>
 * For example, instead of doing this:
 *
 * <p>
 * <pre><code class="language-java">
 *  &#64;PermazenType
 *  public abstract class Event {
 *
 *      protected Event() {
 *          this.setCreateTime(Instant.now());
 *      }
 *
 *      &#64;NotNull
 *      public abstract Instant getCreateTime();
 *      public abstract void setCreateTime(Instant createTime);
 *  }
 * </code></pre>
 *
 * <p>
 * do this:
 * <pre><code class="language-java">
 *  &#64;PermazenType
 *  public abstract class Event {
 *
 *      &#64;NotNull
 *      public abstract Instant getCreateTime();
 *      public abstract void setCreateTime(Instant createTime);
 *
 *      &#64;OnCreate
 *      private void initializeCreateTime() {
 *          this.setCreateTime(Instant.now());
 *      }
 *  }
 * </code></pre>
 *
 * <p><b>Notification Delivery</b></p>
 *
 * <p>
 * Notifications are delivered in the same thread that created the object, immediately after the object is created.
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
 *
 * @see OnDelete
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Documented
public @interface OnCreate {
}
