
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.demo;

import org.jsimpledb.annotation.JSimpleClass;

/**
 * Represents a moon of a planet.
 */
@JSimpleClass
public abstract class Moon extends AbstractBody implements Satellite<Planet> {
}

