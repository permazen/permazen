
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.demo;

import io.permazen.annotation.PermazenType;

/**
 * Represents a planet.
 */
@PermazenType
public abstract class Planet extends AbstractHasSatellites<Moon> implements Satellite<Star> {

    protected Planet() {
        super(Moon.class);
    }

    /**
     * Get whether this planet is ringed.
     *
     * @return true if this planet has rings
     */
    public abstract boolean isRinged();
    public abstract void setRinged(boolean ringed);
}

