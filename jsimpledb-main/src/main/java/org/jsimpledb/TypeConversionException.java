
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import org.jsimpledb.core.DatabaseException;

/**
 * Exception thrown when an object is upgraded, some simple field's type changes, the field is
 * {@linkplain org.jsimpledb.annotation.JField#conversionPolicy configured} with {@link TypeConversionPolicy#REQUIRE},
 * and automated conversion of the field's value to the new type fails.
 *
 * @see TypeConversionPolicy
 * @see org.jsimpledb.annotation.JField#conversionPolicy
 */
@SuppressWarnings("serial")
public class TypeConversionException extends DatabaseException {

    private final JObject jobj;
    private final JSimpleField field;

    /**
     * Constructor.
     *
     * @param jobj object being upgraded
     * @param storageId field's storage ID
     * @param description description of the field
     */
    public TypeConversionException(JObject jobj, JSimpleField field, String message) {
        super(message);
        this.jobj = jobj;
        this.field = field;
    }

    /**
     * Get the ID of the object containing the field whose value could not be converted.
     *
     * @return object ID
     */
    public JObject getJObject() {
        return this.jobj;
    }

    /**
     * Get the the field whose value could not be converted.
     *
     * @return field
     */
    public JSimpleField getField() {
        return this.field;
    }
}

