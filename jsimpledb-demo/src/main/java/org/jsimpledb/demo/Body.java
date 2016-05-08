
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.demo;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.dellroad.stuff.vaadin7.ProvidesProperty;
import org.jsimpledb.JObject;
import org.jsimpledb.annotation.JField;
import org.jsimpledb.vaadin.JObjectContainer;

/**
 * Implemented by all heavenly bodies.
 */
public interface Body extends JObject {

    /**
     * Get the name of this instance.
     *
     * @return name of this object
     */
    @JField(indexed = true)
    @ProvidesProperty(JObjectContainer.REFERENCE_LABEL_PROPERTY)
    @NotNull
    String getName();
    void setName(String name);

    /**
     * Get the mass of this instance in kilograms.
     *
     * @return mass of this object
     */
    @JField(indexed = true)
    @Min(0)
    float getMass();
    void setMass(float name);

    /**
     * Get the image of this instance, if any. Currently this must be a PNG file.
     *
     * @return image of this object, or null if there is none
     */
    byte[] getImage();
    void setImage(byte[] image);
}

