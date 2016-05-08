
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.demo;

import java.util.NavigableSet;

import org.jsimpledb.JObject;
import org.jsimpledb.util.NavigableSets;

/**
 * Support superclass for {@link HasSatellites} implementations.
 */
public abstract class AbstractHasSatellites<S extends Satellite<?>> extends AbstractBody implements HasSatellites<S> {

    private final Class<S> satelliteType;

    protected AbstractHasSatellites(Class<S> satelliteType) {
        this.satelliteType = satelliteType;
    }

    @Override
    public NavigableSet<S> getSatellites() {
        final NavigableSet<S> satellites = this.getTransaction().queryIndex(
          this.satelliteType, "parent", JObject.class).asMap().get(this);
        return satellites != null ? satellites : NavigableSets.<S>empty();
    }
}

