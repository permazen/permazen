
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.demo;

import java.util.NavigableSet;

/**
 * Implemented by heavenly bodies that can have other heavenly bodies as satellites.
 *
 * @param <S> satellites' type
 */
public interface HasSatellites<S extends Satellite<?>> extends Body {

    /**
     * Get the satellites associated with this instance.
     *
     * @return set of satellites, possibly empty
     */
    NavigableSet<S> getSatellites();
}

