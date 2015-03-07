
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.demo;

import org.jsimpledb.annotation.JSimpleClass;

/**
 * Represents a planet.
 */
@JSimpleClass
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

