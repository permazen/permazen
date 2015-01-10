
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.demo;

import org.dellroad.stuff.vaadin7.ProvidesProperty;
import org.jsimpledb.JObject;
import org.jsimpledb.annotation.JField;
import org.jsimpledb.gui.JObjectContainer;

/**
 * Implemented by all heavenly bodies.
 */
public interface Body extends JObject {

    /**
     * Get the name of this instance.
     */
    @JField(indexed = true)
    @ProvidesProperty(JObjectContainer.REFERENCE_LABEL_PROPERTY)
    String getName();
    void setName(String name);

    /**
     * Get the mass of this instance in kilograms.
     */
    @JField(indexed = true)
    float getMass();
    void setMass(float name);
}

