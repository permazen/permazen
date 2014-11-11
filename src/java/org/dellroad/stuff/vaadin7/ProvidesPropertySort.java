
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares that a Java method provides either a {@link Comparable} value or a {@link java.util.Comparator} that
 * should be used to determine property sort ordering in a Java class whose instances back the items in a
 * {@link com.vaadin.data.Container}.
 *
 * <p>
 * {@link ProvidesProperty &#64;ProvidesProperty} and {@link ProvidesPropertySort &#64;ProvidesPropertySort} method annotations
 * can be used to automatically generate a list of {@link PropertyDef}s and a {@link PropertyExtractor} using a
 * {@link ProvidesPropertyScanner}. This happens automatically when using the appropriate constructors of the various
 * {@link com.vaadin.data.Container} classes in this package.
 * </p>
 *
 * <p>
 * This annotation is used in conjunction with the {@link ProvidesProperty &#64;ProvidesProperty}
 * annotation when the value returned by the {@link ProvidesProperty &#64;ProvidesProperty}-annotated method is not
 * {@link Comparable} or does not naturally sort as desired. By declaring a
 * {@link ProvidesPropertySort &#64;ProvidesPropertySort}-annotated method for a property, any arbitrary sorting
 * function can be supplied.
 * </p>
 *
 * <p>
 * If the annotated method returns a {@link java.util.Comparator}, then the return value will be used to sort property values;
 * otherwise, the annotated method must return a sub-type of {@link Comparable}, in which case the returned value determines how
 * that instance's property value sorts (with {@code null} values sorting first). If the method returns neither a
 * {@link java.util.Comparator} nor a {@link Comparable}, an exception is thrown during scanning.
 * </p>
 *
 * <p>
 * Here is a typical situation where {@link ProvidesPropertySort &#64;ProvidesPropertySort} is needed: you have
 * a {@link String} property containing a formatted {@link java.util.Date}, but the way the {@link java.util.Date}
 * strings are formatted does not sort in chronological order. This will usually be the case unless your
 * {@link java.util.Date} strings are formatted like {@code 2013-03-12}, etc.
 * </p>
 *
 * <p>
 * To address this problem, define a {@link ProvidesPropertySort &#64;ProvidesPropertySort}-annotated method that
 * provides a properly sorting {@link Comparable} value corresponding to the associated property. For example:
 * <blockquote><pre>
 * // Container backing object class
 * public class User {
 *
 *     private Date birthday;
 *
 *     <b>&#64;ProvidesPropertySort</b>             // property name "birthday" is implied by method name
 *     public Date getBirthday() {
 *         return this.birthday;
 *     }
 *     public void setBirthday(Date birthday) {
 *         this.birthday = birthday;
 *     }
 *
 *     <b>&#64;ProvidesProperty("birthday")</b>     // the actual property value is returned here
 *     private Label birthdayProperty() {
 *         return new Label(new SimpleDateFormat("MM/dd/yyyy").format(this.birthday));
 *     }
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * Alternately, have the {@link ProvidesPropertySort &#64;ProvidesPropertySort}-annotated method return
 * a {@link java.util.Comparator}, for example, if you wanted to sort {@code null} values last instead of first:
 * <blockquote><pre>
 *     <b>&#64;ProvidesPropertySort("birthday")</b>
 *     private static Comparator&lt;User&gt; birthdayComparator() {
 *         return new Comparator&lt;User&gt;(User user1, User user2) {
 *              final Date date1 = user1.getBirthday();
 *              final Date date2 = user2.getBirthday();
 *              if (date1 == null &amp;&amp; date2 != null)
 *                  return 1;
 *              if (date1 != null &amp;&amp; date2 == null)
 *                  return -1;
 *              if (date1 == null &amp;&amp; date2 == null)
 *                  return 0;
 *              return date1.compareTo(date2);
 *         };
 *     }
 * </pre></blockquote>
 * Note that the returned {@link java.util.Comparator} compares <i>backing instances</i>, not property values,
 * and that methods returning {@link java.util.Comparator} may be declared {@code static}. The returned
 * {@link java.util.Comparator} may be cached by the implementation.
 * </p>
 *
 * <p>
 * Some details regarding {@link ProvidesPropertySort &#64;ProvidesPropertySort} annotations on methods:
 *  <ul>
 *  <li>Only non-void methods taking zero parameters are supported; {@link ProvidesPropertySort &#64;ProvidesPropertySort}
 *      annotations on other methods are ignored</li>
 *  <li>Protected, package private, and private methods are supported.</li>
 *  <li>{@link ProvidesPropertySort &#64;ProvidesPropertySort} annotations on interface methods are supported</li>
 *  <li>If a method and the superclass or superinterface method it overrides are both annotated with
 *      {@link ProvidesPropertySort &#64;ProvidesPropertySort}, then the overridding method's annotation takes precedence.
 *  <li>If two methods with different names are annotated with {@link ProvidesPropertySort &#64;ProvidesPropertySort} for the same
 *      {@linkplain #value property name}, then the declaration in the class which is a sub-type of the other
 *      wins (if the two methods are delcared in the same class, an exception is thrown). This allows subclasses
 *      to "override" which method supplies the sort value for a given property.</li>
 *  </ul>
 * </p>
 *
 *
 * @see ProvidesProperty
 * @see ProvidesPropertyScanner
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.FIELD })
@Documented
public @interface ProvidesPropertySort {

    /**
     * Get the name of the Vaadin property. If this is left unset (empty string), then the
     * bean property name of the annotated bean property "getter" method is used.
     */
    String value() default "";
}

