
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

/**
 * Implemented by classes that can generate a set of differences between themselves and other instances.
 *
 * @param <T> the type being compared
 */
@FunctionalInterface
public interface DiffGenerating<T> {

    /**
     * Detect the differences of this instance when compared to the given instance.
     *
     * @param other other instance
     * @return differences; will be empty if there are none detected
     * @throws IllegalArgumentException if {@code other} is null
     */
    Diffs differencesFrom(T other);
}

