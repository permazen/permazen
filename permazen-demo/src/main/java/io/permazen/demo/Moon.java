
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.demo;

import io.permazen.annotation.JSimpleClass;

/**
 * Represents a moon of a planet.
 */
@JSimpleClass
public abstract class Moon extends AbstractBody implements Satellite<Planet> {
}

