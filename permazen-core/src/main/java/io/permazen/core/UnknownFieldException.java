
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown when an unknown field is accessed.
 */
@SuppressWarnings("serial")
public class UnknownFieldException extends DatabaseException {

    private final ObjType type;
    private final String fieldName;

    /**
     * Constructor.
     *
     * @param type containing object type, or null for none
     * @param fieldName unknown field name
     * @param description description of the unknown field
     */
    public UnknownFieldException(ObjType type, String fieldName, String description) {
        super(String.format("%s has no %s named \"%s\"", type, description, fieldName));
        this.type = type;
        this.fieldName = fieldName;
    }

    /**
     * Constructor.
     *
     * @param fieldName unknown field name
     * @param message exception message
     */
    public UnknownFieldException(String fieldName, String message) {
        super(message);
        this.type = null;
        this.fieldName = fieldName;
    }

    /**
     * Get the object in which the field was not found, if any.
     *
     * @return containing object type, or null if this error is not specific to one object type
     */
    public ObjType getObjType() {
        return this.type;
    }

    /**
     * Get the name that was not recognized.
     *
     * @return unrecognized field name
     */
    public String getFieldName() {
        return this.fieldName;
    }
}
