
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Holds a non-null {@link Enum} value without actually referencing any Java {@link Enum} type.
 * Instead, instances hold a name and an optional ordinal value.
 *
 * @see org.jsimpledb.EnumConverter
 */
public class EnumValue {

    private final int ordinal;          // -1 means unspecified
    private final String name;

    /**
     * Constructor taking only name. Ordinal value will remain unspecified.
     *
     * @param name enum name
     * @throws IllegalArgumentException if {@code name} is null
     */
    public EnumValue(String name) {
        this(name, -1);
    }

    /**
     * Constructor taking name and ordinal value.
     *
     * @param name enum name
     * @param ordinal enum ordinal value, or -1 to leave ordinal value unspecified
     * @throws IllegalArgumentException if {@code ordinal} is less than -1
     * @throws IllegalArgumentException if {@code name} is null
     */
    public EnumValue(String name, int ordinal) {
        if (name == null)
            throw new IllegalArgumentException("null name");
        if (ordinal < -1)
            throw new IllegalArgumentException("invalid ordinal " + ordinal);
        this.name = name;
        this.ordinal = ordinal;
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
     * @return enum value ordinal, or -1 if ordinal is unspecified
     */
    public int getOrdinal() {
        return this.ordinal;
    }

// Object

    @Override
    public String toString() {
        return this.name + (this.ordinal != -1 ? "#" + this.ordinal : "");
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

