
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Holds a non-null {@link Enum} value without actually referencing any Java {@link Enum} type.
 *
 * @see org.jsimpledb.EnumConverter
 */
public class EnumValue {

    private final int ordinal;
    private final String name;

    /**
     * Constructor.
     *
     * @param ordinal enum ordinal value
     * @param name enum name
     * @throws IllegalArgumentException if {@code ordinal} is negative
     * @throws IllegalArgumentException if {@code name} is null
     */
    public EnumValue(int ordinal, String name) {
        if (ordinal < 0)
            throw new IllegalArgumentException("null ordinal");
        if (name == null)
            throw new IllegalArgumentException("null name");
        this.ordinal = ordinal;
        this.name = name;
    }

    /**
     * Constructor taking an actual enum value.
     *
     * @param value enum value
     * @throws IllegalArgumentException if {@code value} is null
     */
    public EnumValue(Enum<?> value) {
        if (value == null)
            throw new IllegalArgumentException("null value");
        this.ordinal = value.ordinal();
        this.name = value.name();
    }

    /**
     * Get the enum name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the enum ordinal value.
     */
    public int getOrdinal() {
        return this.ordinal;
    }

// Object

    @Override
    public String toString() {
        return this.name + "#" + this.ordinal;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode() ^ this.ordinal;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final EnumValue that = (EnumValue)obj;
        return this.name.equals(that.name) && this.ordinal == that.ordinal;
    }
}

