
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.Counter;
import io.permazen.DetachedJTransaction;
import io.permazen.ReferencePath;
import io.permazen.change.FieldChange;
import io.permazen.change.SetFieldAdd;
import io.permazen.change.SetFieldChange;
import io.permazen.change.SimpleFieldChange;
import io.permazen.core.Transaction;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates methods to be invoked whenever some target field in some target object changes during a transaction.
 *
 * <p><b>Overview</b></p>
 *
 * <p>
 * When the value of a matching field in a matching object changes, a change event is created and the annotated
 * method is invoked. The change event's type will be some sub-type of {@link FieldChange} appropriate for the
 * type of field and the change that occurred. Only change events whose types are compatible with the method's
 * parameter are delivered.
 *
 * <p>
 * A "matching object" is one that is found at the end of the {@linkplain ReferencePath reference path} specified
 * by {@link #path}, starting from the object to be notified; see {@link ReferencePath} for more information about
 * reference paths.
 *
 * <p>
 * By default, the reference path is empty, which means changes in the target object itself are monitored.
 *
 * <p>
 * A "matching field" is one named in {@link #value}, or every event-generating field if {@link #value} is empty.
 *
 * <p>
 * A class may have multiple {@link OnChange &#64;OnChange} methods, each with a specific purpose.
 *
 * <p><b>Examples</b></p>
 *
 * <pre>
 *   &#64;PermazenType
 *   public abstract class Account implements JObject {
 *
 *   // Database fields
 *
 *       public abstract boolean isEnabled();
 *       public abstract void setEnabled(boolean enabled);
 *
 *       &#64;NotNull
 *       public abstract String getName();
 *       public abstract void setName(String name);
 *
 *       public abstract NavigableSet&lt;AccessLevel&gt; getAccessLevels();
 *
 *   // &#64;OnChange methods
 *
 *       &#64;OnChange
 *       private void handleAnyChange1(FieldChange&lt;Account&gt; change) {
 *           // Sees any change to ANY field of THIS account
 *       }
 *
 *       &#64;OnChange
 *       private static void handleAnyChange2(FieldChange&lt;Account&gt; change) {
 *           // Sees any change to ANY field of ANY account (note static method)
 *       }
 *
 *       &#64;OnChange("accessLevels")
 *       private void handleAccessLevelsChange(SetFieldAdd&lt;Account, AccessLevel&gt; change) {
 *           // Sees any addition to THIS accounts access levels
 *       }
 *
 *       &#64;OnChange
 *       private void handleSimpleChange(SimpleFieldChange&lt;Account, ?&gt; change) {
 *           // Sees any change to any SIMPLE field of THIS account (i.e., "enabled", "name")
 *       }
 *   }
 *
 *   &#64;PermazenType
 *   public abstract class User implements JObject {
 *
 *   // Database fields
 *
 *       &#64;NotNull
 *       &#64;JField(indexed = true, unique = true)
 *       public abstract String getUsername();
 *       public abstract void setUsername(String username);
 *
 *       &#64;NotNull
 *       public abstract Account getAccount();
 *       public abstract void setAccount(Account account);
 *
 *       public abstract NavigableSet&lt;User&gt; getFriends();
 *
 *   // &#64;OnChange methods
 *
 *       &#64;OnChange("username")
 *       private void handleUsernameChange(SimpleFieldChange&lt;User, String&gt; change) {
 *           // Sees any change to THIS user's username
 *       }
 *
 *       &#64;OnChange(path = "account", value = "enabled")
 *       private void handleUsernameChange(SimpleFieldChange&lt;Account, Boolean&gt; change) {
 *           // Sees any change to THIS user's account's enabled status
 *       }
 *
 *       &#64;OnChange(path = "-&gt;friends-&gt;friends-&gt;account")
 *       private void handleFOFAccountNameChange(SimpleFieldChange&lt;Account, ?&gt; change) {
 *           // Sees any change to ANY simple field in ANY friend-of-a-friend's Account
 *       }
 *
 *       &#64;OnChange(path = "-&gt;account&lt;-User.account", value = "username")
 *       private void handleSameAccountUserUsernameChange(SimpleFieldChange&lt;User, String&gt; change) {
 *           // Sees changes to the username of any User having the same Account as this User.
 *           // Note the use of the inverse step "&lt;-User.account" from Account back to User
 *       }
 *
 *       &#64;OnChange("account")
 *       private static void handleMembershipChange(SimpleFieldChange&lt;User, Account&gt; change) {
 *           // Sees any change to ANY user's account
 *       }
 *   }
 * </pre>
 *
 * <p><b>Method Parameter Types</b></p>
 *
 * <p>
 * In all cases the annotated method must return void and take one parameter whose type must be compatible
 * with at least one of the {@link FieldChange} sub-types appropriate for the/a field being monitored. The
 * parameter type can be narrowed to restrict which notifications are delivered. For example, a method with a
 * {@link SetFieldChange} parameter will receive notifications about all changes to a set field, but a method
 * with a {@link SetFieldAdd} parameter will receive notification only when an element is added to the set.
 *
 * <p>
 * The method may have any level of access, including {@code private}, and multiple independent {@link OnChange &#64;OnChange}
 * methods are allowed.
 *
 * <p>
 * Multiple fields in the target object may be specified; if so, all of the fields are monitored together, and they all
 * must emit {@link FieldChange}s compatible with the method's parameter type. Therefore, when multiple fields are monitored
 * by the same method, the method's parameter type may need to be widened to accomodate them all.
 *
 * <p>
 * If {@link #value} is empty (the default), then every field in the target object is monitored,
 * though again only changes compatible with the method's parameter type will be delivered. So for example, a method
 * taking a {@link SetFieldChange} would receive notifications about changes to all {@code Set} fields in the class,
 * but not any other fields.
 *
 * <p>
 * Currently, due to type erasure, only the parameter's raw type is taken into consideration and an error is generated
 * if the parameter's generic type and its raw type don't match the same events.
 *
 * <p><b>Instance vs. Static Methods</b></p>
 *
 * <p>
 * For an instance method, the method will be invoked on each object for which the changed field is
 * found at the end of the specified reference path starting from that object. So if there are three
 * {@code Child} objects, and the {@code Child} class has an instance method annotated with
 * {@link OnChange &#64;OnChange}{@code (path = "parent", value = "name")}, then all three {@code Child} objects
 * will be notified when the parent's name changes.
 *
 * <p>
 * If the instance is a static method, then the method is invoked once when any instance of the class containing
 * the method exists for which the changed field is found at the end of the specified reference path, no matter how many
 * such instances there are. So in the previous example, making the method static would cause it to be invoked once
 * when the parent's name changes.
 *
 * <p><b>Notification Delivery</b></p>
 *
 * <p>
 * Notifications are delivered synchronously within the thread the made the change, after the change is made and just
 * prior to returning to the original caller.
 * Additional changes made within an {@link OnChange &#64;OnChange} handler that themselves result in notifications
 * are also handled prior to returning to the original caller. Put another way, the queue of outstanding notifications
 * triggered by invoking a method that changes any field is emptied before that method returns. Therefore, infinite loops
 * are possible if an {@link OnChange &#64;OnChange} handler method modifies the field it's monitoring (directly, or indirectly via
 * other {@link OnChange &#64;OnChange} handler methods).
 *
 * <p>
 * {@link OnChange &#64;OnChange} functions within a single transaction; it does not notify about changes that
 * occur in other transactions.
 *
 * <p><b>Fields of Sub-Types</b>
 *
 * <p>
 * The same field can appear in multiple sub-types, e.g., when implementing a Java interface containing a Permazen field.
 * This can lead to some subtleties: for example, in some cases, a field may not exist in a Java object type, but it does
 * exist in a some sub-type of that type:
 *
 * <pre>
 * &#64;PermazenType
 * public abstract class <b>Person</b> {
 *
 *     public abstract Set&lt;Person&gt; <b>getFriends</b>();
 *
 *     &#64;OnChange(path = "friends", field="<b>name</b>")
 *     private void friendNameChanged(SimpleFieldChange&lt;NamedPerson, String&gt; change) {
 *         // ... do whatever
 *     }
 * }
 *
 * &#64;PermazenType
 * public abstract class <b>NamedPerson</b> extends Person {
 *
 *     public abstract String <b>getName</b>();
 *     public abstract void setName(String name);
 * }
 * </pre>
 *
 * Here the path {@code "friends.name"} seems incorrect because {@code "friends"} has type {@code Person},
 * while {@code "name"} is a field of {@code NamedPerson}, a narrower type than {@code Person}. However, this will still
 * work as long as there is no ambiguity, i.e., in this example, there are no other sub-types of {@code Person} with a
 * different field named {@code "name"}.
 *
 * <p>
 * Note also in the example above the {@link SimpleFieldChange} parameter to the method {@code friendNameChanged()}
 * necessarily has generic type {@code NamedPerson}, not {@code Person}.
 *
 * <p><b>Other Notes</b></p>
 *
 * <p>
 * {@link Counter} fields do not generate change notifications.
 *
 * <p>
 * No notifications are delivered for "changes" that do not actually change anything (e.g., setting a simple field to
 * the value already contained in that field, or adding an element to a set which is already contained in the set).
 *
 * <p>
 * For any given field change and path, only one notification will be delivered per recipient object, even if the changed field
 * is seen through the path in multiple ways (e.g., via reference path {@code "mylist.myfield"} where the changed object
 * containing {@code myfield} appears multiple times in {@code mylist}).
 *
 * <p>
 * Some notifications may need to be ignored by objects in {@linkplain DetachedJTransaction detached} transactions;
 * you can use {@code this.isDetached()} to detect that situation.
 *
 * <p>
 * When handing change events, any action that has effects visible to the outside world should be made contingent on
 * successful transaction commit, for example, by wrapping it in {@link Transaction#addCallback Transaction.addCallback()}.
 *
 * <p>
 * See {@link Transaction#addSimpleFieldChangeListener Transaction.addSimpleFieldChangeListener()}
 * for further information on other special corner cases.
 *
 * <p><b>Meta-Annotations</b></p>
 *
 * <p>
 * This annotation may be configured indirectly as a Spring
 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#beans-meta-annotations">meta-annotation</a>
 * when {@code spring-core} is on the classpath.
 *
 * @see ReferencePath
 * @see io.permazen.change
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Documented
public @interface OnChange {

    /**
     * Specify the reference path to the target object(s) that should be monitored for changes.
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

    /**
     * Specify the fields in the target object(s) that should be monitored for changes.
     *
     * <p>
     * Multiple fields may be specified; if so, each field is handled as a separate independent listener registration,
     * and for each field, the method's parameter type must be compatible with at least one of the {@link FieldChange}
     * event sub-types emitted by that field.
     *
     * <p>
     * If zero paths are specified (the default), every field in the target object(s) that emits
     * {@link FieldChange}s compatible with the method's parameter type will be monitored for changes.
     *
     * @return the names of the fields to monitored in the target objects
     */
    String[] value() default { };
}
