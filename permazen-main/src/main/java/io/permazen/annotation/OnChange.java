
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.Counter;
import io.permazen.DetachedPermazenTransaction;
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
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/prism.min.js"></script>
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/components/prism-java.min.js"></script>
 * <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/themes/prism.min.css" rel="stylesheet"/>
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
 * <p>
 * This example shows how the declaration of the annotated method helps determine which events are delivered:
 *
 * <pre><code class="language-java">
 *   &#64;PermazenType
 *   public abstract class Account implements PermazenObject {
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
 *           // Sees any addition to THIS account's access levels
 *       }
 *
 *       &#64;OnChange
 *       private void handleSimpleChange(SimpleFieldChange&lt;Account, ?&gt; change) {
 *           // Sees any change to any SIMPLE field of THIS account (i.e., "enabled", "name")
 *       }
 *   }
 * </code></pre>
 *
 * <p>
 * This example shows how to use the {@link #path} property to track changes in other objects:
 *
 * <pre><code class="language-java">
 *   &#64;PermazenType
 *   public abstract class User implements PermazenObject {
 *
 *   // Database fields
 *
 *       &#64;NotNull
 *       &#64;PermazenField(indexed = true, unique = true)
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
 * </code></pre>
 *
 * <p><b>Custom Indexes</b></p>
 *
 * <p>
 * {@link OnChange &#64;OnChange} annotations are useful for creating custom database indexes. In the most general
 * sense, an "index" is some secondary data structure that (a) is entirely derived from some primary data, (b) can be
 * efficiently when the primary data changes, and (c) provides a quick answer to a question that would otherwise require
 * an extensive calculation if computed from scratch.
 *
 * <p>
 * The code below shows an example index that tracks the average and variance in {@code House} prices.
 * See <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm">Welford's
 * Algorithm</a> for an explanation of how {@code HouseStats.adjust()} works.
 *
 * <pre><code class="language-java">
 *  &#64;PermazenType
 *  public abstract class House implements PermazenObject {
 *
 *      public abstract double getPrice();
 *      public abstract void setPrice(double price);
 *
 *  // Index maintenance
 *
 *      &#64;OnCreate
 *      private void handleAddition() {
 *          this.getStats().adjust(true, this.getPrice());    // price is always zero here
 *      }
 *
 *      &#64;OnDelete
 *      private void handleRemoval() {
 *          this.getStats().adjust(false, this.getPrice());
 *      }
 *
 *      &#64;OnChange("price")
 *      private void handlePriceChange(SimpleFieldChange&lt;House, Double&gt; change) {
 *          HouseStats stats = this.getStats();
 *          stats.adjust(false, change.getOldValue());
 *          stats.adjust(true, change.getNewValue());
 *      }
 *
 *      private HouseStats getStats() {
 *          return this.getPermazenTransaction().getSingleton(HouseStats.class);
 *      }
 *  }
 *
 *  &#64;PermazenType(singleton = true)
 *  public abstract class HouseStats implements PermazenObject {
 *
 *  // Public Methods
 *
 *      // The total number of houses
 *      public abstract long getCount();
 *
 *      // The average house price
 *      public abstract double getAverage();
 *
 *      // The variance in the house price
 *      public double getVariance() {
 *          return this.getCount() &gt; 1 ? this.getM2() / this.getCount() : 0.0;
 *      }
 *
 *  // Non-public Methods
 *
 *      protected abstract void setCount(long count);
 *
 *      protected abstract void setAverage(double average);
 *
 *      protected abstract double getM2();
 *      protected abstract void setM2(double m2);
 *
 *      protected void adjust(boolean add, double price) {
 *          long count = this.getCount();
 *          double mean = this.getAverage();
 *          double m2 = this.getM2();
 *          double delta;
 *          double delta2;
 *          if (add) {
 *              count++;
 *              delta = price - mean;
 *              mean += delta / count;
 *              delta2 = price - mean;
 *              m2 += delta * delta2;
 *          } else if (count == 1) {
 *              count = 0;
 *              mean = 0;
 *              m2 = 0;
 *          } else {                    // reverse the above steps
 *              delta2 = price - mean;
 *              mean = (count * mean - price) / (count - 1);
 *              delta = price - mean;
 *              m2 -= delta * delta2;
 *              count--;
 *          }
 *          this.setCount(count);
 *          this.setAverage(mean);
 *          this.setM2(m2);
 *      }
 *  }
 * </code></pre>
 *
 * <p><b>Depenedent Objects</b></p>
 *
 * <p>
 * {@link OnChange &#64;OnChange} annotations can be used to automatically garbage collect dependent objects.
 * A dependent object is one that is only useful or meaningful in the context of some other object(s) that reference it.
 * You can combine {@link OnChange &#64;OnChange} with {@link OnDelete &#64;OnDelete} and {@link ReferencePath &#64;ReferencePath}
 * to keep track of incoming references.
 *
 * <p>
 * For example, suppose multiple {@code Person}'s can share a common {@code Address}. You only want {@code Address} objects
 * in your database when they are referred to by at least one {@code Person}. As soon as an {@code Address} is no longer
 * referenced, you want it to be automaticaly garbage collected.
 *
 * <p>
 * Then you could do something like this:
 *
 * <pre><code class="language-java">
 *   &#64;PermazenType
 *   public abstract class Person implements PermazenObject {
 *
 *       &#64;NotNull
 *       public abstract String getName();
 *       public abstract void setName(String name);
 *
 *       &#64;NotNull
 *       public abstract Address getAddress();
 *       public abstract void setAddress(Address address);
 *
 *       &#64;OnDelete
 *       private void onDelete() {
 *           this.setAddress(null); // ensure we notify Address.referenceChange()
 *       }
 *   }
 *
 *   &#64;PermazenType
 *   public abstract class Address implements PermazenObject {
 *
 *       &#64;NotNull
 *       public abstract String getAddress();
 *       public abstract void setAddress(String adddress);
 *
 *       &#64;NotNull
 *       public abstract String getCity();
 *       public abstract void setCity(String city);
 *
 *       &#64;NotNull
 *       public abstract String getZip();
 *       public abstract void setZip(String zip);
 *
 *   // Index queries
 *
 *       &#64;ReferencePath("&lt;-Person.address")
 *       public abstract NavigableSet&lt;Person&gt; getOccupants();
 *
 *   // &#64;OnChange methods
 *
 *       &#64;OnChange(path = "&lt;-Person.address", value = "address")
 *       private void referenceChange(SimpleFieldChange&lt;Person, Address&gt; change) {
 *           if (this.getOccupants().isEmpty())
 *               this.delete();
 *       }
 *   }
 * </code></pre>
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
 * <pre><code class="language-java">
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
 * </code></pre>
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
 * Some notifications may need to be ignored by objects in {@linkplain DetachedPermazenTransaction detached} transactions;
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
 * <a href="https://docs.spring.io/spring-framework/reference/core/beans/classpath-scanning.html#beans-meta-annotations">meta-annotation</a>
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
