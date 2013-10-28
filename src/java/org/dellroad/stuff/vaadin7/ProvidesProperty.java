
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
 * Annotates a Java field or method that provides a read-only Vaadin {@link com.vaadin.data.Property} value.
 *
 * <p>
 * This annotation indicates that a read-only Vaadin {@link com.vaadin.data.Property} having the {@linkplain #value specified name}
 * and type derived from the annotated field or zero-argument method is accessible by reading that field or method.
 * </p>
 *
 * <p>
 * Classes with {@link ProvidesProperty &#64;ProvidesProperty}-annotated fields and methods can be used to automatically
 * generate a {@link PropertyExtractor} and a list of {@link PropertyDef}s by using a {@link PropertyReader}.
 * </p>
 *
 * <p>
 * The {@link com.vaadin.data.Container} classes in this package that are configured with a {@link PropertyExtractor}
 * and a list of {@link PropertyDef}s all have a constructor variant taking a {@link ProvidesProperty
 * &#64;ProvidesProperty}-annotated Java class, which allows the container properties to be auto-detected via introspection.
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
 *     &#64;ProvidesProperty(REAL_NAME_PROPERTY)
 *     public String getRealName() {
 *         return this.realName;
 *     }
 *     public void setRealName(String realName) {
 *         this.realName = realName;
 *     }
 *
 *     &#64;ProvidesProperty(USERNAME_PROPERTY)
 *     public Label usernameProperty() {
 *         return new Label("&lt;code&gt;" + StringUtil.escapeHtml(this.username) + "&lt;/code&gt;", ContentMode.HTML);
 *     }
 * }
 *
 * // User container class
 * public class UserContainer extends SimpleKeyedContainer&lt;String, User&gt; {
 *
 *     public UserContainer() {
 *         super(User.class);
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
 * container.load(this.getAllUsers());
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
 *  <li>{@link ProvidesProperty &#64;ProvidesProperty} annotations on interface methods are supported</li>
 *  <li>If a method and the superclass or superinterface method it overrides are both annotated with
 *      {@link ProvidesProperty &#64;ProvidesProperty}, but the annotations declare different properties
 *      (i.e., they have different {@linkplain #value names}), then two properties will be defined with the same value</li>
 *  <li>After introspection, all property names must be unique, with the exception that a method may redundantly
 *      declare the same property (i.e., having the same {@linkplain #value name}) as any method it overrides.</li>
 *  </ul>
 * </p>
 *
 * @see PropertyReader
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.FIELD })
@Documented
public @interface ProvidesProperty {

    /**
     * Get the name of the Vaadin property. If this is left unset (empty string), then the name of the
     * annotated field, or the bean property name of the annotated "getter" method is used.
     */
    String value() default "";
}

