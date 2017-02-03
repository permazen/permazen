
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import org.jsimpledb.core.DatabaseException;
import org.jsimpledb.core.ObjId;

/**
 * Exception thrown when an object is upgraded, some simple field's type changes, the field is
 * {@linkplain org.jsimpledb.annotation.JField#upgradeConversion configured} with {@link UpgradeConversionPolicy#REQUIRE},
 * and automated conversion of the field's value to the new type fails.
 *
 * @see UpgradeConversionPolicy
 * @see org.jsimpledb.annotation.JField#upgradeConversion
 */
@SuppressWarnings("serial")
public class UpgradeConversionException extends DatabaseException {

    private final ObjId id;
    private final int storageId;

    /**
     * Constructor.
     *
     * @param id ID of the object being upgraded
     * @param storageId the storage ID of the field
     * @param message exception message
     */
    public UpgradeConversionException(ObjId id, int storageId, String message) {
        this(id, storageId, message, null);
    }

    /**
     * Constructor.
     *
     * @param id ID of the object being upgraded
     * @param storageId the storage ID of the field
     * @param message exception message
     * @param cause underlying exception, or null if none
     */
    public UpgradeConversionException(ObjId id, int storageId, String message, Throwable cause) {
        super(message, cause);
        this.id = id;
        this.storageId = storageId;
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
     * Get the the field to which the old value could not be converted.
     *
     * @return the storage ID of the field
     */
    public int getFieldStorageId() {
        return this.storageId;
    }
}

