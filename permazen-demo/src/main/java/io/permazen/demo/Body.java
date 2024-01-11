
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.demo;

import io.permazen.PermazenObject;
import io.permazen.annotation.PermazenField;
import io.permazen.vaadin.JObjectContainer;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import org.dellroad.stuff.vaadin7.ProvidesProperty;

/**
 * Implemented by all heavenly bodies.
 */
public interface Body extends PermazenObject {

    /**
     * Get the name of this instance.
     *
     * @return name of this object
     */
    @PermazenField(indexed = true)
    @ProvidesProperty(JObjectContainer.REFERENCE_LABEL_PROPERTY)
    @NotNull
    String getName();
    void setName(String name);

    /**
     * Get the mass of this instance in kilograms.
     *
     * @return mass of this object
     */
    @PermazenField(indexed = true)
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
