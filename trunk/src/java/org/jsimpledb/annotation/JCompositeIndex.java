
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Java annotation defining a composite index on a {@link JSimpleClass}-annotated class.
 *
 * <p>
 * A composite index is an index on two or more fields (to define a single-field index,
 * just set {@link JField#indexed} to true). All fields indexed in a composite index
 * must be (a) simple and (b) not a sub-field of a complex field.
 * </p>
 *
 * @see JSimpleClass
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({})
@Documented
public @interface JCompositeIndex {

    /**
     * The name of this composite index. Must be globally unique.
     */
    String name();

    /**
     * The storage ID for this composite index. Value must be positive.
     */
    int storageId();

    /**
     * The names of the indexed fields, in the desired order. At least two fields must be specified.
     */
    String[] fields();
}

