
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.PermazenType;

import java.lang.reflect.Method;

/**
 * Used to auto-generate storage ID's for Java model classes, fields, and composite indexes when not specified
 * explicitly in the corresponding {@link PermazenType &#64;PermazenType}, {@link JField &#64;JField}, etc., annotations.
 *
 * @see PermazenFactory#setStorageIdGenerator PermazenFactory.setStorageIdGenerator()
 */
public interface StorageIdGenerator {

    /**
     * Generate a storage ID for a Java model class.
     *
     * @param type Java model class
     * @param typeName database type name
     * @return generated storage ID
     */
    int generateClassStorageId(Class<?> type, String typeName);

    /**
     * Generate a storage ID for a composite index.
     *
     * @param type Java model class containing the indexed fields
     * @param name composite index name
     * @param fields indexed field storage ID's in index order
     * @return generated storage ID
     */
    int generateCompositeIndexStorageId(Class<?> type, String name, int[] fields);

    /**
     * Generate a storage ID for a regular top-level field (i.e., a field that is not a sub-field of a complex field).
     *
     * @param getter the field's Java bean getter method
     * @param name the field's database name
     * @return generated storage ID
     */
    int generateFieldStorageId(Method getter, String name);

    /**
     * Generate a storage ID for a set field.
     *
     * @param getter the set field's Java bean getter method
     * @param name the set element field's database name
     * @return generated storage ID
     */
    int generateSetElementStorageId(Method getter, String name);

    /**
     * Generate a storage ID for a list field.
     *
     * @param getter the list field's Java bean getter method
     * @param name the list element field's database name
     * @return generated storage ID
     */
    int generateListElementStorageId(Method getter, String name);

    /**
     * Generate a storage ID for a map key field.
     *
     * @param getter the map field's Java bean getter method
     * @param name the map key field's database name
     * @return generated storage ID
     */
    int generateMapKeyStorageId(Method getter, String name);

    /**
     * Generate a storage ID for a map value field.
     *
     * @param getter the map field's Java bean getter method
     * @param name the map value field's database name
     * @return generated storage ID
     */
    int generateMapValueStorageId(Method getter, String name);
}
