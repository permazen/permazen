
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.DetachedPermazenTransaction;
import io.permazen.ReferencePath;
import io.permazen.core.Transaction;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates Permazen model class methods that are to be invoked whenever an object is about to be deleted.
 *
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/prism.min.js"></script>
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/components/prism-java.min.js"></script>
 * <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/themes/prism.min.css" rel="stylesheet"/>
 *
 * <p><b>Overview</b></p>
 *
 * <p>
 * When a <i>matching object</i> is deleted, annotated methods are invoked just prior to actual deletion.
 *
 * <p>
 * Methods must return void and normally take one parameter. A matching object is one whose type is compatible
 * with the method parameter and which is found at the end of the {@linkplain ReferencePath reference path}
 * specified by {@link #path}, starting from the object to be notified. See {@link ReferencePath} for more
 * information about reference paths.
 *
 * <p>
 * In the case of an instance method where {@link #path} is empty (the default), the method is allowed to take
 * zero parameters; in this case, the object monitors itself.
 *
 * <p>
 * For static methods, {@link #path} must be empty and every object whose type is compatible with the method's parameter
 * is a matching object and can produce notifications.
 *
 * <p>
 * The annotated method may may have any level of access, including {@code private}.
 *
 * <p>
 * A class may have multiple {@link OnDelete &#64;OnDelete} methods, each with a specific purpose.
 *
 * <p><b>Examples</b></p>
 *
 * <p>
 * This example shows various ways an annotated method can be matched:
 *
 * <pre><code class="language-java">
 *   &#64;PermazenType
 *   public abstract class User implements PermazenObject {
 *
 *       public abstract Account getAccount();
 *       public abstract void setAccount(Account account);
 *
 *       &#64;OnDelete(path = "-&gt;account")
 *       private void handleDeletion1(Account account) {
 *           // Invoked when MY Account is deleted
 *       }
 *   }
 *
 *   &#64;PermazenType
 *   public abstract class Feature implements PermazenObject {
 *
 *       public abstract Account getAccount();
 *       public abstract void setAccount(Account account);
 *
 *       &#64;OnDelete(path = "-&gt;account&lt;-User.account")
 *       private void handleDeletion1(User user) {
 *           // Invoked when ANY User associated with MY Account is deleted
 *       }
 *   }
 *
 *   &#64;PermazenType
 *   public abstract class Account implements PermazenObject {
 *
 *       &#64;NotNull
 *       public abstract String getName();
 *       public abstract void setName(String name);
 *
 *   // Non-static &#64;OnDelete methods
 *
 *       &#64;OnDelete
 *       private void handleDeletion1() {
 *           // Invoked when THIS Account is deleted
 *       }
 *
 *       &#64;OnDelete
 *       private void handleDeletion2(SpecialAccount self) {
 *           // Invoked when THIS Account is deleted IF it's also a SpecialAccount
 *       }
 *
 *       &#64;OnDelete(path = "&lt;-User.account")
 *       private void handleDeletion3(User user) {
 *           // Invoked when ANY User associated with THIS Account is deleted
 *       }
 *
 *       &#64;OnDelete(path = "&lt;-User.account")
 *       private void handleDeletion4(Object obj) {
 *           // Invoked when ANY User OR Feature associated with THIS Account is deleted
 *       }
 *
 *       &#64;OnDelete(path = "&lt;-Feature.account-&gt;account&lt;-User.account")
 *       private void handleDeletion5(User user) {
 *           // Invoked when ANY User associated with the same Account
 *           // as ANY Feature associated with THIS Account is deleted
 *       }
 *
 *  // Static &#64;OnDelete methods
 *
 *       &#64;OnDelete
 *       private static void handleDeletion5(Object obj) {
 *           // Invoked when ANY object is deleted
 *       }
 *
 *       &#64;OnDelete
 *       private static void handleDeletion6(Account account) {
 *           // Invoked when ANY Account is deleted
 *       }
 *   }
 * </code></pre>
 *
 * <p><b>Instance vs. Static Methods</b></p>
 *
 * <p>
 * An instance method will be invoked on <i>each object</i> for which the deleted object is found at the end
 * of the specified reference path, starting from that object. For example, if there are three child {@code Node}'s
 * pointing to the same parent {@code Node}, and the {@code Node} class has an instance method annotated with
 * {@link OnDelete &#64;OnDelete}{@code (path = "parent")}, then all three child {@code Node}'s will be notified
 * when the parent is deleted.
 *
 * <p>
 * A static method is invoked <i>once</i> for any matching object; the {@link path} is ignored and must be empty.
 *
 * <p><b>Notification Delivery</b></p>
 *
 * <p>
 * Notifications are delivered in the same thread that deletes the object, before the deletion actually occurs.
 * At most one delete notification will ever be delivered for any object deletion event. In particular, if an
 * annotated method attempts to re-entrantly delete the same object again, no new notification is delivered.
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
 * @see OnCreate
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Documented
public @interface OnDelete {

    /**
     * Specify the reference path to the target object(s) that should be monitored for deletion.
     * See {@link ReferencePath} for information on reference paths and their proper syntax.
     *
     * <p>
     * The default empty path means the monitored object and the notified object are the same. In that case,
     * the type of the parameter (if any) restricts notifications to compatible subclasses.
     *
     * <p>
     * When annotating static methods, this property is unused and must be left unset.
     *
     * @return reference path leading to monitored objects
     * @see ReferencePath
     */
    String path() default "";
}
