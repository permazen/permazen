
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.annotation.PermazenField;
import io.permazen.core.DatabaseException;
import io.permazen.core.ObjId;

/**
 * Exception thrown when an object is upgraded, some simple field's type changes, the field is
 * {@linkplain PermazenField#upgradeConversion configured} with {@link UpgradeConversionPolicy#REQUIRE},
 * and automated conversion of the field's value to the new type fails.
 *
 * @see UpgradeConversionPolicy
 * @see PermazenField#upgradeConversion
 */
@SuppressWarnings("serial")
public class UpgradeConversionException extends DatabaseException {

    private final ObjId id;
    private final String fieldName;

    /**
     * Constructor.
     *
     * @param id ID of the object being upgraded
     * @param fieldName the name of the field
     * @param message exception message
     */
    public UpgradeConversionException(ObjId id, String fieldName, String message) {
        this(id, fieldName, message, null);
    }

    /**
     * Constructor.
     *
     * @param id ID of the object being upgraded
     * @param fieldName the name of the field
     * @param message exception message
     * @param cause underlying exception, or null if none
     * @throws IllegalArgumentException if {@code id} or {@code fieldName} is null
     */
    public UpgradeConversionException(ObjId id, String fieldName, String message, Throwable cause) {
        super(message, cause);
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(fieldName != null, "null fieldName");
        this.id = id;
        this.fieldName = fieldName;
    }

    /**
     * Get the ID of the object containing the field whose value could not be converted.
     *
     * @return object ID
     */
    public ObjId getObjId() {
        return this.id;
    }

    /**
     * Get the name of the field from which the old value could not be converted.
     *
     * @return the name of the field
     */
    public String getFieldName() {
        return this.fieldName;
    }
}
