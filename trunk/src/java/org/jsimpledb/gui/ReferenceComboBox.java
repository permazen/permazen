
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.shared.ui.combobox.FilteringMode;
import com.vaadin.ui.AbstractSelect;
import com.vaadin.ui.ComboBox;

import org.jsimpledb.JSimpleDB;

/**
 * {@link ComboBox} that chooses an object reference.
 */
@SuppressWarnings("serial")
public class ReferenceComboBox extends ComboBox {

    public ReferenceComboBox(JSimpleDB jdb, Class<?> type, boolean allowNull) {
        final ObjectContainer container = new ObjectContainer(jdb, type);
        this.setContainerDataSource(container);
        this.setNullSelectionAllowed(allowNull);
        this.setNewItemsAllowed(false);
        this.setTextInputAllowed(false);
        this.setFilteringMode(FilteringMode.OFF);
        this.setItemCaptionMode(AbstractSelect.ItemCaptionMode.PROPERTY);
        this.setItemCaptionPropertyId(ObjectContainer.REFERENCE_LABEL_PROPERTY);
        if (allowNull)
            this.setInputPrompt("Null");
    }
}

