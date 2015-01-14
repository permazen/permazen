
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.ui.DefaultFieldFactory;
import com.vaadin.ui.Table;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.Session;

/**
 * Table showing all objects of a certain type, backed by an {@link ObjectContainer}.
 */
@SuppressWarnings("serial")
public class ObjectTable extends AbstractTable<JObjectContainer> {

    private final JSimpleDB jdb;
    private final JObjectContainer container;
    private final Session session;
    private final boolean showFields;

    public ObjectTable(JSimpleDB jdb, JObjectContainer container, Session session) {
        this(jdb, container, session, true);
    }

    public ObjectTable(JSimpleDB jdb, JObjectContainer container, Session session, boolean showFields) {
        if (jdb == null)
            throw new IllegalArgumentException("null jdb");
        if (container == null)
            throw new IllegalArgumentException("null container");
        if (session == null)
            throw new IllegalArgumentException("null session");
        this.jdb = jdb;
        this.container = container;
        this.session = session;
        this.showFields = showFields;
        this.setSelectable(true);
        this.setImmediate(true);
        this.setSizeFull();
    }

    @Override
    protected JObjectContainer buildContainer() {
        return this.container;
    }

    @Override
    protected void configureColumns() {
        this.setColumnCollapsingAllowed(true);
        for (String propertyName : this.getContainer().getOrderedPropertyNames()) {
            String title = DefaultFieldFactory.createCaptionByPropertyId(propertyName);
            Table.Align align = Table.Align.CENTER;
            int width = 120;
            boolean showField = this.showFields;
            switch (propertyName) {
            case JObjectContainer.REFERENCE_LABEL_PROPERTY:
                title = "Label";
                width = 120;
                showField = true;
                break;
            case JObjectContainer.OBJECT_ID_PROPERTY:
                title = "ID";
                width = 120;
                showField = false;
                break;
            case JObjectContainer.TYPE_PROPERTY:
                title = "Type";
                width = 80;
                break;
            case JObjectContainer.VERSION_PROPERTY:
                title = "Version";
                width = 40;
                showField = false;
                break;
            default:
                break;
            }
            this.addColumn(propertyName, title, width, align);
            this.setColumnExpandRatio(propertyName, width / 120.0f);
            this.setColumnCollapsed(propertyName, !showField);
        }
    }
}

