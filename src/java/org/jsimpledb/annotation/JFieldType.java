
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Java annotation for classes that define custom {@link org.jsimpledb.core.FieldType}s
 * for a {@link org.jsimpledb.JSimpleDB}'s underlying {@link org.jsimpledb.core.Database}.
 * These classes will be automatically instantiated and registered with the
 * {@linkplain org.jsimpledb.core.Database#getFieldTypeRegistry associated}
 * {@link org.jsimpledb.core.FieldTypeRegistry}.
 *
 * <p>
 * Annotated classes must extend {@link org.jsimpledb.core.FieldType} and have a zero-parameter constructor.
 * </p>
 *
 * <p>
 * Note that once a certain encoding has been used for a given type name in a database, the encoding should not
 * be changed without creating a new type (and type name), or else {@link org.jsimpledb.core.InconsistentDatabaseException}s
 * could result when the new type attempts to decode the old encoding or vice-versa.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface JFieldType {

    /**
     * The name under which to register this field type. Must be unique among all field types in any
     * {@link org.jsimpledb.core.FieldTypeRegistry}.
     *
     * <p>
     * If equal to the empty string (default value),
     * the string value of the value returned by {@linkplain org.jsimpledb.core.FieldType#getTypeToken} is used.
     * </p>
     */
    String name() default "";
}

