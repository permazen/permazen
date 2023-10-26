
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.Counter;
import io.permazen.ReferencePath;
import io.permazen.SnapshotJTransaction;
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
 * Annotation for methods that are to be invoked whenever a simple or complex target field in some target object changes
 * during a transaction, where the target object containing the changed field is found at the end of a path of references
 * starting from the object to be notified.
 * See {@link ReferencePath} for more information about reference paths.
 *
 * <p><b>Overview</b></p>
 *
 * There several ways to control which changes are delivered to the annotated method:
 * <ul>
 *  <li>By specifying a path of object references, via {@link #value}, to the target object and field</li>
 *  <li>By widening or narrowing the type of the {@link FieldChange} method parameter
 *      (or omitting it altogether)</li>
 *  <li>By declaring an instance method, to monitor changes from the perspective of the associated object,
 *      or a static method, to monitor changes from a global perspective</li>
 *  <li>By {@linkplain #snapshotTransactions allowing or disallowing} notifications that occur within
 *      {@linkplain SnapshotJTransaction snapshot transactions}.</li>
 * </ul>
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
 *       &#64;OnChange      // equivalent to &#64;OnChange("*")
 *       private void handleAnyChange1() {
 *           // Sees any change to ANY field of THIS account
 *       }
 *
 *       &#64;OnChange("*")
 *       private void handleAnyChange2(FieldChange&lt;Account&gt; change) {
 *           // Sees any change to ANY field of THIS account
 *       }
 *
 *       &#64;OnChange("*")
 *       private static void handleAnyChange3(FieldChange&lt;Account&gt; change) {
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
 *           // Sees any change to any SIMPLE field of THIS account (e.g., enabled, name)
 *       }
 *
 *       &#64;OnChange(startType = User.class, value = "account")
 *       private static void handleMembershipChange(SimpleFieldChange&lt;User, Account&gt; change) {
 *           // Sees any change to which users are associated with ANY account
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
 *       &#64;OnChange("account.enabled")
 *       private void handleUsernameChange(SimpleFieldChange&lt;Account, Boolean&gt; change) {
 *           // Sees any change to THIS user's account's enabled status
 *       }
 *
 *       &#64;OnChange("friends.element.friends.element.account.*")
 *       private void handleFOFAccountNameChange(SimpleFieldChange&lt;Account, ?&gt; change) {
 *           // Sees any change to any simple field in any friend-of-a-friend's Account
 *       }
 *
 *       &#64;OnChange("account.^User:account^.username")
 *       private void handleSameAccountUserUsernameChange(SimpleFieldChange&lt;User, String&gt; change) {
 *           // Sees changes to the username of any User with the same Account as this instance
 *           // Note the use of the inverse step "^User:account^" from Account back to User
 *       }
 *   }
 * </pre>
 *
 * <p><b>Method Parameter Types</b></p>
 *
 * <p>
 * In all cases the annotated method must return void and take zero or one parameter; the parameter must be compatible
 * with at least one of the {@link FieldChange} sub-types appropriate for the field being watched.
 * The method parameter type can be used to restrict which notifications are delivered. For example, an annotated method
 * taking a {@link SetFieldChange} will receive notifications about all changes to a set field,
 * while a method taking a {@link SetFieldAdd} will receive notification only when an element is added to the set.
 *
 * <p>
 * A method with zero parameters is delivered all possible notifications, which is equivalent to having an ignored
 * parameter of type {@link FieldChange FieldChange&lt;?&gt;}.
 *
 * <p>
 * The method may have any level of access, including {@code private}, and multiple independent {@link OnChange &#64;OnChange}
 * methods are allowed.
 *
 * <p>
 * Multiple reference paths may be specified; if so, all of the specified paths are monitored together, and they all
 * must emit {@link FieldChange}s compatible with the method's parameter type. Therefore, when
 * multiple fields are monitored, the method's parameter type may need to be widened (either in raw type, generic type
 * parameters, or both).
 *
 * <p>
 * As a special case, if the last field is {@code "*"} (wildcard), then every field in the target object is matched.
 * However, only fields that emit changes compatible with the method's parameter type will be monitored.
 * So for example, a method taking a {@link SetFieldChange} would receive notifications about
 * changes to all {@code Set} fields in the class, but not any other fields. Currently, due to type erasure, only
 * the parameter's raw type is taken into consideration.
 *
 * <p><b>Instance vs. Static Methods</b></p>
 *
 * <p>
 * If the method is an instance method, then {@link #startType} must be left unset; if the instance is a static
 * method, then {@link #startType} may be explicitly set, or if left unset it defaults to the class containing
 * the annotated method.
 *
 * <p>
 * For an instance method, the method will be invoked on <i>each object</i> for which the changed field is
 * found at the end of the specified reference path <i>starting from that object</i>.
 *
 * <p>
 * If the annotated method is a static method, the method is invoked <i>once</i> if any instance exists for which the
 * changed field is found at the end of the specified reference path, no matter how many such instances there are.
 * Otherwise the behavior is the same.
 *
 * <p><b>Notification Delivery</b></p>
 *
 * <p>
 * Notifications are delivered synchronously within the thread the made the change, after the change is made and just
 * prior to returning to the original caller.
 * Additional changes made within an {@link OnChange &#64;OnChange} handler that themselves result in notifications
 * are also handled prior to returning to the original caller. Therefore, infinite loops are possible if an
 * {@link OnChange &#64;OnChange} handler method modifies the field it's monitoring (directly, or indirectly via
 * other {@link OnChange &#64;OnChange} handler methods).
 *
 * <p>
 * {@link OnChange &#64;OnChange} functions within a single transaction; it does not notify about changes that
 * may have occurred in a different transaction.
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
 * public class Person {
 *
 *     public abstract Set&lt;Person&gt; <b>getFriends</b>();
 *
 *     &#64;OnChange("friends.element.<b>name</b>")
 *     private void friendNameChanged(SimpleFieldChange&lt;NamedPerson, String&gt; change) {
 *         // ... do whatever
 *     }
 * }
 *
 * &#64;PermazenType
 * public class NamedPerson extends Person {
 *
 *     public abstract String <b>getName</b>();
 *     public abstract void setName(String name);
 * }
 * </pre>
 *
 * Here the path {@code "friends.element.name"} seems incorrect because {@code "friends.element"} has type {@code Person},
 * while {@code "name"} is a field of {@code NamedPerson}, a narrower type than {@code Person}. However, this will still
 * work as long as there is no ambiguity, i.e., in this example, there are no other sub-types of {@code Person} with a field
 * named {@code "name"}. Note also in the example above the {@link SimpleFieldChange} parameter to the
 * method {@code friendNameChanged()} necessarily has generic type {@code NamedPerson}, not {@code Person}.
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
 * is seen through the path in multiple ways (e.g., via reference path {@code "mylist.element.myfield"} where the changed object
 * containing {@code myfield} appears multiple times in {@code mylist}).
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
     * Specifies the path(s) to the target field(s) to watch for changes.
     * See {@link ReferencePath} for information on the proper syntax.
     *
     * <p>
     * Multiple paths may be specified; if so, each path is handled as a separate independent listener registration,
     * and the method's parameter type must be compatible with at least one of the {@link FieldChange}
     * sub-types emitted by each field.
     *
     * <p>
     * If zero paths are specified (the default), every field in the class (including superclasses) that emits
     * {@link FieldChange}s compatible with the method parameter will be monitored for changes.
     *
     * @return reference path leading to the changed field
     * @see ReferencePath
     */
    String[] value() default { };

    /**
     * Specifies the starting type for the {@link ReferencePath} specified by {@link #value}.
     *
     * <p>
     * This property must be left unset for instance methods. For static methods, if this property is left unset,
     * then then class containing the annotated method is assumed.
     *
     * @return Java type at which the reference path starts
     * @see ReferencePath
     */
    Class<?> startType() default void.class;

    /**
     * Determines whether this annotation should also be enabled for
     * {@linkplain SnapshotJTransaction snapshot transaction} objects.
     * If unset, notifications will only be delivered to non-snapshot (i.e., normal) database instances.
     *
     * @return whether enabled for snapshot transactions
     * @see SnapshotJTransaction
     */
    boolean snapshotTransactions() default false;
}
