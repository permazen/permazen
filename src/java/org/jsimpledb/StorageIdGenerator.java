
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.lang.reflect.Method;

/**
 * Used to auto-generate storage ID's for Java model classes, fields, and composite indexes when not specified
 * explicitly in the corresponding {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass},
 * {@link org.jsimpledb.annotation.JField &#64;JField}, etc., annotations.
 *
 * @see JSimpleDBFactory#setStorageIdGenerator JSimpleDBFactory.setStorageIdGenerator()
 */
public interface StorageIdGenerator {

    /**
     * Generage a storage ID for a Java model class.
     *
     * @param type Java model class
     * @param typeName database type name
     */
    int generateClassStorageId(Class<?> type, String typeName);

    /**
     * Generage a storage ID for a composite index.
     *
     * @param type Java model class containing the indexed fields
     * @param name composite index name
     * @param fields indexed field storage ID's in index order
     */
    int generateCompositeIndexStorageId(Class<?> type, String name, int[] fields);

    /**
     * Generage a storage ID for a regular field.
     *
     * @param getter the field's Java bean getter method
     * @param name the field's database name
     */
    int generateFieldStorageId(Method getter, String name);

    /**
     * Generage a storage ID for a set field.
     *
     * @param getter the field's Java bean getter method
     * @param name the field's database name
     */
    int generateSetElementStorageId(Method getter, String name);

    /**
     * Generage a storage ID for a list field.
     *
     * @param getter the field's Java bean getter method
     * @param name the field's database name
     */
    int generateListElementStorageId(Method getter, String name);

    /**
     * Generage a storage ID for a map key field.
     *
     * @param getter the field's Java bean getter method
     * @param name the field's database name
     */
    int generateMapKeyStorageId(Method getter, String name);

    /**
     * Generage a storage ID for a map value field.
     *
     * @param getter the field's Java bean getter method
     * @param name the field's database name
     */
    int generateMapValueStorageId(Method getter, String name);
}

