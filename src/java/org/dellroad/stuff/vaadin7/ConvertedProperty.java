
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.data.Property;
import com.vaadin.data.util.AbstractProperty;
import com.vaadin.data.util.converter.Converter;

import java.util.Locale;

/**
 * Applies a {@link Converter} to a {@link Property} of one type (the "model" type) to produce a
 * {@link Property} of another type (the "presentation" type).
 *
 * @param <P> the "presentation" type of this {@link Property}
 * @param <M> the "model" type of the nested {@link Property}
 */
@SuppressWarnings("serial")
public class ConvertedProperty<P, M> extends AbstractProperty<P> {

    private final Property<M> property;
    private final Converter<P, M> converter;
    private final Locale locale;

    private ValueChangeListener valueChangeListener;
    private ReadOnlyStatusChangeListener readOnlyStatusChangeListener;

    /**
     * Primary constructor.
     *
     * @param property underlying property
     * @param converter converts this property's values from/to the underlying property's values
     * @param locale the {@link Locale} to provide to {@code converter}, or null for none
     * @throws IllegalArgumentException if {@code type}, {@code property}, or {@code converter} is null
     */
    public ConvertedProperty(Property<M> property, Converter<P, M> converter, Locale locale) {
        if (property == null)
            throw new IllegalArgumentException("null property");
        if (converter == null)
            throw new IllegalArgumentException("null converter");
        this.property = property;
        this.converter = converter;
        this.locale = locale;
    }

    /**
     * Convenience constructor for when no {@link Locale} is required.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><code>
     *  ConvertedProperty(property, converter, null)
     *  </code></blockquote>
     */
    public ConvertedProperty(Property<M> property, Converter<P, M> converter) {
        this(property, converter, null);
    }

    @Override
    public Class<? extends P> getType() {
        return this.converter.getPresentationType();
    }

    @Override
    public P getValue() {
        return this.converter.convertToPresentation(this.property.getValue(), this.locale);
    }

    @Override
    public void setValue(Object value) {
        this.property.setValue(this.converter.convertToModel(this.getType().cast(value), this.locale));
    }

    @Override
    public boolean isReadOnly() {
        return this.property.isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        this.property.setReadOnly(readOnly);
    }

    @Override
    public void addListener(Property.ReadOnlyStatusChangeListener listener) {
        if (this.readOnlyStatusChangeListener == null && this.property instanceof Property.ReadOnlyStatusChangeNotifier) {
            this.readOnlyStatusChangeListener = new ReadOnlyStatusChangeListener();
            ((Property.ReadOnlyStatusChangeNotifier)this.property).addListener(this.readOnlyStatusChangeListener);
        }
        super.addListener(listener);
    }

    @Override
    public void addListener(Property.ValueChangeListener listener) {
        if (this.valueChangeListener == null && this.property instanceof Property.ValueChangeNotifier) {
            this.valueChangeListener = new ValueChangeListener();
            ((Property.ValueChangeNotifier)this.property).addListener(this.valueChangeListener);
        }
        super.addListener(listener);
    }

// Property.ValueChangeListener relay

    @SuppressWarnings("serial")
    private class ValueChangeListener implements Property.ValueChangeListener {
        @Override
        public void valueChange(Property.ValueChangeEvent event) {
            ConvertedProperty.this.fireValueChange();
        }
    }

// Property.ReadOnlyStatusChangeListener relay

    @SuppressWarnings("serial")
    private class ReadOnlyStatusChangeListener implements Property.ReadOnlyStatusChangeListener {
        @Override
        public void readOnlyStatusChange(Property.ReadOnlyStatusChangeEvent event) {
            ConvertedProperty.this.fireReadOnlyStatusChange();
        }
    }
}

