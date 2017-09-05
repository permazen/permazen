
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.demo;

import com.vaadin.server.StreamResource;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Component;
import com.vaadin.ui.Field;
import com.vaadin.ui.Image;

import java.io.ByteArrayInputStream;

import org.dellroad.stuff.vaadin7.BlobField;
import org.dellroad.stuff.vaadin7.FieldBuilder;
import org.dellroad.stuff.vaadin7.ProvidesProperty;
import io.permazen.vaadin.NullableField;
import io.permazen.vaadin.SizedLabel;

/**
 * Support superclass for {@code Body} implementations.
 */
public abstract class AbstractBody implements Body {

// Object

    // Show the database object ID instead of system hash code
    @Override
    public String toString() {
        return this.getClass().getName() + "@" + this.getObjId();
    }

// Vaadin GUI customizations

    // The @ProvidesProperty annotation here indicates that this method should be invoked
    // to build the Vaadin property named "image" instead of doing the default thing (using toString()).
    // We are converting the byte[] value into a Vaadin PNG Image object.

    @ProvidesProperty("image")
    @SuppressWarnings("serial")
    private Component propertyImage() {
        final byte[] imageData = this.getImage();
        if (imageData == null)
            return new SizedLabel("<i>Null</i>", ContentMode.HTML);
        return new Image(null, new StreamResource(new StreamResource.StreamSource() {
            @Override
            public ByteArrayInputStream getStream() {
                return new ByteArrayInputStream(imageData);
            }
        }, "image.png"));
    }

    // The @FieldBuilder.ProvidesField annotation indicates that if anyone wants to edit
    // the "image" field, they should use this method to build an appropriate Vaaddin Field instead
    // of doing the default thing (i.e., using a TextField).

    @FieldBuilder.ProvidesField("image")
    private Field<byte[]> buildImagePropertyField() {
        return new NullableField<byte[]>(new BlobField());
    }
}

