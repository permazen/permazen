
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Holds a non-null {@link Enum} value without actually referencing any Java {@link Enum} type.
 * Instead, instances hold a name and ordinal value.
 *
 * @see org.jsimpledb.EnumConverter
 */
public class EnumValue {

    private final String name;
    private final int ordinal;

    /**
     * Constructor taking name and ordinal value.
     *
     * @param name enum name
     * @param ordinal enum ordinal value
     * @throws IllegalArgumentException if {@code ordinal} is negative
     * @throws IllegalArgumentException if {@code name} is null
     */
    public EnumValue(String name, int ordinal) {
        if (name == null)
            throw new IllegalArgumentException("null name");
        if (ordinal < 0)
            throw new IllegalArgumentException("invalid negative ordinal " + ordinal);
        this.name = name;
        this.ordinal = ordinal;
    }

    /**
     * Constructor taking an {@link Enum} value.
     *
     * @param value enum value
     * @throws IllegalArgumentException if {@code value} is null
     */
    public EnumValue(Enum<?> value) {
        if (value == null)
            throw new IllegalArgumentException("null value");
        this.name = value.name();
        this.ordinal = value.ordinal();
    }

    /**
     * Get the enum name.
     *
     * @return enum value name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the enum ordinal value.
     *
     * @return enum value ordinal
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

