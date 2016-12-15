
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

/**
 * Implemented by classes that are capable of estimating their memory usage.
 */
@FunctionalInterface
public interface SizeEstimating {

    /**
     * Add the estimated size of this instance (in bytes) to the given estimator.
     *
     * @param estimator size estimator
     */
    void addTo(SizeEstimator estimator);
}

