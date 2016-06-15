
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
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
 *
 * @see JSimpleClass
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({})
@Documented
public @interface JCompositeIndex {

    /**
     * The name of this composite index. Must be globally unique.
     *
     * @return the index name
     */
    String name();

    /**
     * The storage ID for this composite index. Value should be positive; if zero, the configured
     * {@link org.jsimpledb.StorageIdGenerator} will be consulted to auto-generate a value.
     *
     * @return the index storage ID
     * @see org.jsimpledb.StorageIdGenerator#generateCompositeIndexStorageId StorageIdGenerator.generateCompositeIndexStorageId()
     */
    int storageId() default 0;

    /**
     * The names of the indexed fields, in the desired order. At least two fields must be specified.
     *
     * @return the names of the indexed fields
     */
    String[] fields();
}

