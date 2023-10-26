
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

/**
 * Upper and lower bound types.
 */
public enum BoundType {

    /**
     * The bound is inclusive.
     */
    INCLUSIVE(true),

    /**
     * The bound is exclusive.
     */
    EXCLUSIVE(false),

    /**
     * There is no bound.
     */
    NONE(null);

    private final Boolean booleanValue;

    BoundType(Boolean booleanValue) {
        this.booleanValue = booleanValue;
    }

    /**
     * Return{@link BoundType#INCLUSIVE} or {@link BoundType#EXCLUSIVE} based on the given boolean.
     *
     * @param inclusive true for {@link BoundType#INCLUSIVE}, false for {@link BoundType#EXCLUSIVE}
     * @return the corresponding {@link BoundType}
     */
    public static BoundType of(boolean inclusive) {
        return inclusive ? BoundType.INCLUSIVE : BoundType.EXCLUSIVE;
    }

    /**
     * Get a {@link Boolean} corresponding to this instance: true for {@link #INCLUSIVE},
     * false for {@link #EXCLUSIVE}, or null for {@link #NONE}.
     *
     * @return boolean representation of this bound type
     */
    public Boolean isInclusive() {
        return this.booleanValue;
    }
}
