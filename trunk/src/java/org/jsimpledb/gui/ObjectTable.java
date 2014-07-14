
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.ui.DefaultFieldFactory;
import com.vaadin.ui.Table;

/**
 * Table showing all objects of a certain type, backed by an {@link ObjectContainer}.
 */
@SuppressWarnings("serial")
public class ObjectTable extends AbstractTable<ObjectContainer> {

    private final Class<?> type;
    private final boolean showFields;

    public ObjectTable(Class<?> type) {
        this(type, true);
    }

    public ObjectTable(Class<?> type, boolean showFields) {
        this.type = type;
        this.showFields = showFields;
        this.setSelectable(true);
        this.setImmediate(true);
        this.setSizeFull();
    }

    @Override
    protected ObjectContainer buildContainer() {
        return new ObjectContainer(this.type);
    }

    @Override
    protected void configureColumns() {

        // Add columns
        this.setColumnCollapsingAllowed(true);
        for (String fieldName : this.getContainer().getOrderedPropertyNames()) {
            String title = DefaultFieldFactory.createCaptionByPropertyId(fieldName);
            Table.Align align = Table.Align.CENTER;
            int width = 120;
            boolean showField = this.showFields;
            switch (fieldName) {
            case ObjectContainer.REFERENCE_LABEL_PROPERTY:
                title = "Label";
                width = 120;
                showField = true;
                break;
            case ObjectContainer.OBJ_ID_PROPERTY:
                title = "ID";
                width = 120;
                break;
            case ObjectContainer.TYPE_PROPERTY:
                title = "Type";
                width = 80;
                break;
            case ObjectContainer.VERSION_PROPERTY:
                title = "Version";
                width = 40;
                break;
            default:
                break;
            }
            this.addColumn(fieldName, title, width, align);
            this.setColumnExpandRatio(fieldName, width / 120.0f);
            this.setColumnCollapsed(fieldName, !showField);
        }

        // Adjust columns
        this.setColumnCollapsingAllowed(true);
        if (!this.getContainer().hasReferenceLabel())
            this.setColumnCollapsed(ObjectContainer.REFERENCE_LABEL_PROPERTY, true);
    }
}

