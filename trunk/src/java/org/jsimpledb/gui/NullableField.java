
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.data.Property;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Field;

import org.dellroad.stuff.vaadin7.FieldLayout;

/**
 * Wraps a {@link Field} that is capable of displaying, but not necessarily choosing, a null value,
 * and adds a "Null" button that sets the value to null.
 */
@SuppressWarnings("serial")
public class NullableField<T> extends FieldLayout<T> {

    private final SmallButton nullButton = new SmallButton("Null", new Button.ClickListener() {
        @Override
        public void buttonClick(Button.ClickEvent event) {
            NullableField.this.field.setValue(null);
        }
    });

    public NullableField(Field<T> field) {
        super(field);
        this.setMargin(false);
        this.setSpacing(true);
        this.addComponent(this.field);
        this.addComponent(this.nullButton);
        this.setComponentAlignment(this.nullButton, Alignment.MIDDLE_LEFT);
        this.field.addValueChangeListener(new Property.ValueChangeListener() {
            @Override
            public void valueChange(Property.ValueChangeEvent event) {
                NullableField.this.updateDisplay();
            }
        });
        if (this.field instanceof Property.ReadOnlyStatusChangeNotifier) {
            final Property.ReadOnlyStatusChangeNotifier notifier = (Property.ReadOnlyStatusChangeNotifier)field;
            notifier.addReadOnlyStatusChangeListener(new Property.ReadOnlyStatusChangeListener() {
                @Override
                public void readOnlyStatusChange(Property.ReadOnlyStatusChangeEvent event) {
                    NullableField.this.updateDisplay();
                }
            });
        }
    }

    @Override
    public void attach() {
        super.attach();
        this.updateDisplay();
    }

// Internal methods

    private void updateDisplay() {
        this.nullButton.setEnabled(this.field.getValue() != null && !this.field.isReadOnly());
    }
}

