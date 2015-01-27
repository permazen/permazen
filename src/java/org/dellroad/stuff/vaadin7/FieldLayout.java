
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.data.Buffered;
import com.vaadin.data.Property;
import com.vaadin.data.Validator;
import com.vaadin.ui.Field;
import com.vaadin.ui.HorizontalLayout;

import java.util.Collection;

/**
 * A {@link HorizontalLayout} that also exposes an internally wrapped {@link Field}. This is useful when you
 * have an existing {@link Field} and want to display it differently and/or with additional {@link com.vaadin.ui.Component}s
 * but still need it to function as a {@link Field} from a data perspective.
 *
 * <p>
 * The layout is initially empty; use {@link #addComponent HorizontalLayout.addComponent()} to add content.
 * </p>
 *
 * @param <T> field type
 */
@SuppressWarnings({ "serial", "deprecation" })
public class FieldLayout<T> extends HorizontalLayout implements Field<T> {

    protected final Field<T> field;

    /**
     * Constructor.
     *
     * @param field the wrapped {@link Field} that this instance will derive its {@link Field} state from
     */
    public FieldLayout(Field<T> field) {
        if (field == null)
            throw new IllegalArgumentException("null field");
        this.field = field;
    }

// Field

    @Override
    public String getRequiredError() {
        return this.field.getRequiredError();
    }

    @Override
    public void setRequiredError(String requiredError) {
        this.field.setRequiredError(requiredError);
    }

    @Override
    public boolean isRequired() {
        return this.field.isRequired();
    }

    @Override
    public void setRequired(boolean required) {
        this.field.setRequired(required);
    }

// Component.Focusable

    @Override
    public void focus() {
        this.field.focus();
    }

    @Override
    public int getTabIndex() {
        return this.field.getTabIndex();
    }

    @Override
    public void setTabIndex(int index) {
        this.field.setTabIndex(index);
    }

// BufferedValidatable

    @Override
    public boolean isInvalidCommitted() {
        return this.field.isInvalidCommitted();
    }

    @Override
    public void setInvalidCommitted(boolean invalidCommitted) {
        this.field.setInvalidCommitted(invalidCommitted);
    }

// Buffered

    @Override
    public void commit() throws Buffered.SourceException {
        this.field.commit();
    }

    @Override
    public void discard() throws Buffered.SourceException {
        this.field.discard();
    }

    @Override
    public void setBuffered(boolean buffered) {
        this.field.setBuffered(buffered);
    }

    @Override
    public boolean isBuffered() {
        return this.field.isBuffered();
    }

    @Override
    public boolean isModified() {
        return this.field.isModified();
    }

// Validatable

    @Override
    public void addValidator(Validator validator) {
        this.field.addValidator(validator);
    }

    @Override
    public void removeValidator(Validator validator) {
        this.field.removeValidator(validator);
    }

    @Override
    public void removeAllValidators() {
        this.field.removeAllValidators();
    }

    @Override
    public Collection<Validator> getValidators() {
        return this.field.getValidators();
    }

    @Override
    public boolean isValid() {
        return this.field.isValid();
    }

    @Override
    public void validate() throws Validator.InvalidValueException {
        this.field.validate();
    }

    @Override
    public boolean isInvalidAllowed() {
        return this.field.isInvalidAllowed();
    }

    @Override
    public void setInvalidAllowed(boolean invalidAllowed) {
        this.field.setInvalidAllowed(invalidAllowed);
    }

// Property

    @Override
    public Class<? extends T> getType() {
        return this.field.getType();
    }

    @Override
    public T getValue() {
        return this.field.getValue();
    }

    @Override
    public boolean isReadOnly() {
        return this.field.isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        this.field.setReadOnly(readOnly);
    }

    @Override
    public void setValue(T value) {
        this.field.setValue(value);
    }

// Property.ValueChangeNotifier

    @Override
    public void addValueChangeListener(Property.ValueChangeListener listener) {
        this.field.addValueChangeListener(listener);
    }

    @Override
    public void removeValueChangeListener(Property.ValueChangeListener listener) {
        this.field.removeValueChangeListener(listener);
    }

    @Override
    public void addListener(Property.ValueChangeListener listener) {
        this.field.addValueChangeListener(listener);
    }

    @Override
    public void removeListener(Property.ValueChangeListener listener) {
        this.field.removeValueChangeListener(listener);
    }

// Property.ValueChangeListener

    @Override
    public void valueChange(Property.ValueChangeEvent event) {
        this.field.valueChange(event);
    }

// Property.Viewer

    @Override
    @SuppressWarnings("rawtypes")
    public void setPropertyDataSource(Property newDataSource) {
        this.field.setPropertyDataSource(newDataSource);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Property getPropertyDataSource() {
        return this.field.getPropertyDataSource();
    }
}

