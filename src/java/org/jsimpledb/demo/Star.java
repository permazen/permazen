
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.demo;

import org.jsimpledb.annotation.JSimpleClass;

/**
 * Represents a star.
 */
@JSimpleClass
public abstract class Star extends AbstractHasSatellites<Planet> {

    protected Star() {
        super(Planet.class);
    }

    /**
     * Get this star's luminosity.
     *
     * @return luminosity in Joules per second.
     */
    public abstract float getLuminosity();
    public abstract void setLuminosity(float luminosity);
}

