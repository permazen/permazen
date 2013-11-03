
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
 * Annotates Java methods that provide a {@link java.util.Comparable} value used to determine property sort ordering.
 * When the value returned by a {@link ProvidesProperty &#64;ProvidesProperty}-annotated method is not
 * {@link java.util.Comparable} or does not naturally sort as desired, a value that does can be provided
 * by an associated {@link ProvidesPropertySort &#64;ProvidesPropertySort}-annotated method.
 *
 * <p>
 * {@link ProvidesProperty &#64;ProvidesProperty} and {@link ProvidesPropertySort &#64;ProvidesPropertySort} method annotations
 * can be used to automatically generate a list of {@link PropertyDef}s and a {@link PropertyExtractor} using a
 * {@link ProvidesPropertyScanner}. This happens automatically when using the appropriate constructors of the various
 * {@link com.vaadin.data.Container} classes in this package.
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
 * provides a properly sorting {@link java.util.Comparable} value corresponding to the associated property. For example:
 * <blockquote><pre>
 * // Container backing object class
 * public class User {
 *
 *     private Date birthday;
 *
 *     <b>&#64;ProvidesPropertySort</b>            // property "birthday" is implied by method name
 *     public Date getBirthday() {
 *         return this.birthday;
 *     }
 *     public void setBirthday(Date birthday) {
 *         this.birthday = birthday;
 *     }
 *
 *     <b>&#64;ProvidesProperty("birthday")</b>
 *     private Label birthdayProperty() {
 *         return new Label(new SimpleDateFormat("MM/dd/yyyy").format(this.birthday));
 *     }
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * Note that it doesn't make sense to add {@link ProvidesPropertySort &#64;ProvidesPropertySort} to a method already annotated
 * with {@link ProvidesProperty &#64;ProvidesProperty}, because that just specifies what is already the default behavior.
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
 *  </ul>
 * </p>
 *
 *
 * @see ProvidesPropertyScanner
 * @see ProvidesProperty
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

