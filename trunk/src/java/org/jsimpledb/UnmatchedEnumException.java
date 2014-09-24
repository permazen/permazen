
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import org.jsimpledb.core.EnumValue;

/**
 * Thrown when an orphaned {@link Enum} value that cannot be mapped is encountered.
 *
 * <p>
 * This exception is thrown when a field having {@link Enum} type is read, but the previously stored {@link Enum} value
 * no longer exists in the newer {@link Enum} type associated with the field, and it cannot be mapped by name or ordinal.
 * </p>
 *
 * @see EnumConverter
 */
@SuppressWarnings("serial")
public class UnmatchedEnumException extends JSimpleDBException {

    private final Class<? extends Enum<?>> type;
    private final EnumValue value;

    /**
     * Constructor.
     *
     * @param type Java {@link Enum} model type
     * @param value value obtained from the core database layer
     * @throws IllegalArgumentException if either parameter is null
     */
    public UnmatchedEnumException(Class<? extends Enum<?>> type, EnumValue value) {
        super("no value found in Enum " + type + " matching " + value);
        if (type == null)
            throw new IllegalArgumentException("null type");
        if (value == null)
            throw new IllegalArgumentException("null value");
        this.type = type;
        this.value = value;
    }

    /**
     * Get the {@link Enum} type in which a matching value could not be found.
     */
    public Class<? extends Enum<?>> getType() {
        return this.type;
    }

    /**
     * Get the {@link EnumValue} obtained from the core database layer.
     */
    public EnumValue getValue() {
        return this.value;
    }
}

