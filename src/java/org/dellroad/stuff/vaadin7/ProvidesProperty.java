
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
 * Annotates a Java method that provides a read-only Vaadin {@link com.vaadin.data.Property} value.
 *
 * <p>
 * This annotation indicates that a read-only Vaadin {@link com.vaadin.data.Property} having the {@linkplain #value specified name}
 * and type derived from the method's return value is accessible by reading that method.
 * Annotated methods must have zero parameters.
 * </p>
 *
 * <p>
 * {@link ProvidesProperty &#64;ProvidesProperty} and {@link ProvidesPropertySort &#64;ProvidesPropertySort} method annotations
 * can be used to automatically generate a list of {@link PropertyDef}s and a {@link PropertyExtractor} using a
 * {@link ProvidesPropertyScanner}. This happens automatically when using the appropriate constructors of the various
 * {@link com.vaadin.data.Container} classes in this package.
 * </p>
 *
 * <p>
 * For example:
 * <blockquote><pre>
 * // Container backing object class
 * public class User {
 *
 *     public static final String USERNAME_PROPERTY = "username";
 *     public static final String REAL_NAME_PROPERTY = "realName";
 *
 *     private String username;
 *     private String realName;
 *
 *     public String getUsername() {
 *         return this.username;
 *     }
 *     public void setUsername(String username) {
 *         this.username = username;
 *     }
 *
 *     <b>&#64;ProvidesProperty</b>            // property "realName" is implied by method name
 *     public String getRealName() {
 *         return this.realName;
 *     }
 *     public void setRealName(String realName) {
 *         this.realName = realName;
 *     }
 *
 *     <b>&#64;ProvidesProperty(USERNAME_PROPERTY)</b>
 *     private Label usernameProperty() {
 *         return new Label("&lt;code&gt;" + StringUtil.escapeHtml(this.username) + "&lt;/code&gt;", ContentMode.HTML);
 *     }
 * }
 *
 * // User container class
 * public class UserContainer extends SimpleKeyedContainer&lt;String, User&gt; {
 *
 *     public UserContainer() {
 *         super(<b>User.class</b>);
 *     }
 *
 *     &#64;Override
 *     public String getKeyFor(User user) {
 *         return user.getUsername();
 *     }
 * }
 *
 * // Create container holding all users
 * UserContainer container = new UserContainer();
 * container.load(this.userDAO.getAllUsers());
 *
 * // Build table showing users
 * Table table = new Table();
 * table.setColumnHeader(User.USERNAME_PROPERTY, "User");
 * table.setColumnHeader(User.REAL_NAME_PROPERTY, "Name");
 * table.setVisibleColumns(User.USERNAME_PROPERTY, User.REAL_NAME_PROPERTY);
 *
 * // Select user "jsmith" in the table
 * table.setValue("jsmith");
 * ...
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * Some details regarding {@link ProvidesProperty &#64;ProvidesProperty} annotations on methods:
 *  <ul>
 *  <li>Only non-void methods taking zero parameters are supported; {@link ProvidesProperty &#64;ProvidesProperty}
 *      annotations on other methods are ignored</li>
 *  <li>Protected, package private, and private methods are supported.</li>
 *  <li>{@link ProvidesProperty &#64;ProvidesProperty} annotations on interface methods are supported</li>
 *  <li>If a method and the superclass or superinterface method it overrides are both annotated with
 *      {@link ProvidesProperty &#64;ProvidesProperty}, then the overridding method's annotation takes precedence.
 *  </ul>
 * </p>
 *
 * <p>
 * To control how properties are sorted (e.g., in tables), see {@link ProvidesPropertySort &#64;ProvidesPropertySort}.
 * </p>
 *
 * @see ProvidesPropertyScanner
 * @see ProvidesPropertySort
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.FIELD })
@Documented
public @interface ProvidesProperty {

    /**
     * Get the name of the Vaadin property. If this is left unset (empty string), then the
     * bean property name of the annotated bean property "getter" method is used.
     */
    String value() default "";
}

