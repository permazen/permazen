
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.demo;

import io.permazen.annotation.PermazenType;

/**
 * Represents a moon of a planet.
 */
@PermazenType
public abstract class Moon extends AbstractBody implements Satellite<Planet> {
}

