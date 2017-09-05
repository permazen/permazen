
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.vaadin.data.Property;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Field;

import org.dellroad.stuff.vaadin7.FieldLayout;

/**
 * Wraps a {@link Field} that is capable of displaying, but not necessarily choosing, a null value,
 * and adds a "Null" button that sets the value to null.
 */
@SuppressWarnings("serial")
public class NullableField<T> extends FieldLayout<T> {

    private final SmallButton nullButton = new SmallButton("Null", e -> this.field.setValue(null));

    public NullableField(Field<T> field) {
        super(field);
        this.setMargin(false);
        this.setSpacing(true);
        this.addComponent(this.field);
        this.addComponent(this.nullButton);
        this.setComponentAlignment(this.nullButton, Alignment.MIDDLE_LEFT);
        this.field.addValueChangeListener(e -> this.updateDisplay());
        if (this.field instanceof Property.ReadOnlyStatusChangeNotifier) {
            final Property.ReadOnlyStatusChangeNotifier notifier = (Property.ReadOnlyStatusChangeNotifier)field;
            notifier.addReadOnlyStatusChangeListener(e -> this.updateDisplay());
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

