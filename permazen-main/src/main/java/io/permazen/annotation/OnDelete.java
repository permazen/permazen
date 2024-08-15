
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
 * When a matching object is deleted, annotated methods are invoked just prior to the actual deletion.
 *
 * <p>
 * A "matching object" is one that is found at the end of the {@linkplain ReferencePath reference path} specified
 * by {@link #path}, starting from the object to be notified; see {@link ReferencePath} for more information about
 * reference paths.
 *
 * <p>
 * By default, the reference path is empty, which means deletion of the target object itself is monitored.
 *
 * <p>
 * The annotated method may be an instance method or a static method. It may have any level of access, including
 * {@code private}. It must return void and take one parameter representing the deleted object; however, an instance
 * method annotated with an empty {@link #path} must take zero parameters, because in that case the deleted object is
 * the same as the notified object.
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
 *       &#64;OnDelete(path = "&lt;-User.account")
 *       private void handleDeletion2(User user) {
 *           // Invoked when ANY User associated with THIS Account is deleted
 *       }
 *
 *       &#64;OnDelete(path = "&lt;-User.account")
 *       private void handleDeletion3(Object obj) {
 *           // Invoked when ANY User OR Feature associated with THIS Account is deleted
 *       }
 *
 *       &#64;OnDelete(path = "&lt;-Feature.account-&gt;account&lt;-User.account")
 *       private void handleDeletion4(User user) {
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
 *
 *       &#64;OnDelete(path = "&lt;-User.account")
 *       private static void handleDeletion7(Account account) {
 *           // Invoked when ANY Account having at least one User is deleted
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
 * A static method is invoked <i>once</i> when any instance of the class containing the method exists for which the
 * deleted object is found at the end of the specified reference path, no matter how many such instances there are.
 * So in the previous example, making the method static would cause it to be invoked only once when the parent
 * is deleted (and not at all if there were zero child {@code Node}'s). Put another way, an annotation on a static
 * method behaves just like the same annotation on an instance method, except that multiple per-object notifications
 * are coalesced into a single per-class notification.
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
     * The default empty path means the monitored object and the notified object are the same.
     *
     * <p>
     * In the case of static methods, a non-empty path restricts notification from being delivered
     * unless there exists at least one object for whom the monitored object is found at the other
     * end of the path.
     *
     * @return reference path leading to monitored objects
     * @see ReferencePath
     */
    String path() default "";
}
