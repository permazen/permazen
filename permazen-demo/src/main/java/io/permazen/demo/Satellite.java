
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.demo;

import jakarta.validation.constraints.NotNull;

/**
 * Implemented by heavenly bodies that can orbit around other heavenly bodies.
 *
 * @param <P> parent heavenly body type
 */
public interface Satellite<P extends HasSatellites<?>> extends Body {

    /**
     * Get the parent around which this instance orbits, if any.
     *
     * @return parent heavenly body
     */
    @NotNull
    P getParent();
    void setParent(P parent);
}

