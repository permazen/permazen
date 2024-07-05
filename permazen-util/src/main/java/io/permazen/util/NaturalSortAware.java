
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import java.util.Comparator;

/**
 * Implemented by {@link Comparator} classes that know whether or not their ordering is
 * identical to the target Java type's natural ordering.
 */
public interface NaturalSortAware {

    /**
     * Determine if this instance sorts Java instances naturally.
     *
     * <p>
     * This method should return true only if all of the following are true:
     * <ul>
     *  <li>This class also implements {@link Comparator Comparator<T>} for some Java type {@code T}.
     *  <li>Type {@code T} has a natural ordering (i.e., {@code T} itself implements {@link Comparable}).
     *  <li>The ordering implied by this class's {@link Comparator#compare compare()} method is
     *      identical to {@code T}'s natural ordering.
     * </ul>
     *
     * @return true if this instance orders Java values in their natural order
     */
    boolean sortsNaturally();
}
