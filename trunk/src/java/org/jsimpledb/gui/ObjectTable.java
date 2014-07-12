
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.ui.DefaultFieldFactory;
import com.vaadin.ui.Table;

import org.jsimpledb.JClass;

/**
 * Table showing all objects of a certain type, backed by an {@link ObjectContainer}.
 */
@SuppressWarnings("serial")
public class ObjectTable extends AbstractTable<ObjectContainer> {

    private final Class<?> type;
    private final JClass<?> jclass;

    public ObjectTable(Class<?> type) {
        this(type, null);
    }

    public ObjectTable(JClass<?> jclass) {
        this(null, jclass);
    }

    private ObjectTable(Class<?> type, JClass<?> jclass) {
        this.type = type;
        this.jclass = jclass;
        this.setSelectable(true);
        this.setImmediate(true);
        this.setSizeFull();
    }

    @Override
    protected ObjectContainer buildContainer() {
        return this.jclass != null ? new ObjectContainer(this.jclass) : new ObjectContainer(this.type);
    }

    @Override
    protected void configureColumns() {

        // Add columns
        for (String fieldName : this.getContainer().getOrderedPropertyNames()) {
            String title = DefaultFieldFactory.createCaptionByPropertyId(fieldName);
            Table.Align align = Table.Align.CENTER;
            int width = 120;
            switch (fieldName) {
            case ObjectContainer.REFERENCE_LABEL_PROPERTY:
                title = "Object";
                width = 120;
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
                title = "Ver";
                width = 30;
                break;
            default:
                break;
            }
            this.addColumn(fieldName, title, width, align);
            this.setColumnExpandRatio(fieldName, 120.0f / width);
        }

        // Adjust columns
        this.setColumnCollapsingAllowed(true);
        this.setColumnCollapsed(ObjectContainer.VERSION_PROPERTY, true);
        if (!this.getContainer().hasReferenceLabel())
            this.setColumnCollapsed(ObjectContainer.REFERENCE_LABEL_PROPERTY, true);
    }
}

