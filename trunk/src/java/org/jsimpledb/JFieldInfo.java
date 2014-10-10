
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;

/**
 * Schema-wide information common to all {@link JField}s sharing a storage ID.
 *
 * <p>
 * As the same {@link JField} can appear in multiple object types, this class contains the information about a
 * {@link JField} that is not specific to any one {@link JClass}. This is particularly relevant for reference fields,
 * which can have different Java reference types in different {@link JClass}es.
 * </p>
 */
abstract class JFieldInfo {

    final int storageId;

    JFieldInfo(JField jfield) {
        this.storageId = jfield.storageId;
    }

    /**
     * Get the storage ID of this field.
     */
    public int getStorageId() {
        return this.storageId;
    }

    /**
     * Get a {@link Converter} that converts this field's value from what the core database returns
     * to what the Java application expects, or null if no conversion is needed.
     *
     * <p>
     * The implementation in {@link JFieldInfo} returns null.
     * </p>
     *
     * @param jtx transaction
     */
    public Converter<?, ?> getConverter(JTransaction jtx) {
        return null;
    }

// Object

    @Override
    public String toString() {
        return "field with storage ID " + this.storageId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final JFieldInfo that = (JFieldInfo)obj;
        return this.storageId == that.storageId;
    }

    @Override
    public int hashCode() {
        return this.storageId;
    }
}

