
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.demo;

/**
 * Implemented by heavenly bodies that can orbit around other heavenly bodies.
 *
 * @param <P> parent heavenly body type
 */
public interface Satellite<P extends HasSatellites<?>> extends Body {

    /**
     * Get the parent around which this instance orbits, if any.
     *
     * @return parent heavenly body, or null if this heavenly body is just floating around in space
     */
    P getParent();
    void setParent(P parent);
}

