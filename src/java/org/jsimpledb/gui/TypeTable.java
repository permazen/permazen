
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.ui.Table;
import com.vaadin.ui.TreeTable;

/**
 * Table showing object types.
 */
@SuppressWarnings("serial")
public class TypeTable extends TreeTable {

    public TypeTable(JClassContainer container) {
        super(null, container);

        this.setSelectable(false);
        this.setImmediate(false);
        this.setSizeFull();
        this.setHierarchyColumn(JClassContainer.Node.NAME_PROPERTY);
        this.setAnimationsEnabled(true);

        this.addColumn(JClassContainer.Node.NAME_PROPERTY, "Name", 140, Table.Align.LEFT);
        this.addColumn(JClassContainer.Node.STORAGE_ID_PROPERTY, "SID", 40, Table.Align.CENTER);
        this.addColumn(JClassContainer.Node.TYPE_PROPERTY, "Type", 250, Table.Align.CENTER);

        this.setColumnExpandRatio(JClassContainer.Node.NAME_PROPERTY, 1.0f);

        this.setVisibleColumns(JClassContainer.Node.NAME_PROPERTY,
          JClassContainer.Node.STORAGE_ID_PROPERTY, JClassContainer.Node.TYPE_PROPERTY);
        this.setColumnCollapsingAllowed(true);
        this.setColumnCollapsed(JClassContainer.Node.STORAGE_ID_PROPERTY, true);
        this.setColumnCollapsed(JClassContainer.Node.TYPE_PROPERTY, true);
    }

    protected void addColumn(String property, String name, int width, Table.Align alignment) {
        this.setColumnHeader(property, name);
        this.setColumnWidth(property, width);
        if (alignment != null)
            this.setColumnAlignment(property, alignment);
    }

    public JClassContainer getContainer() {
        return (JClassContainer)this.getContainerDataSource();
    }

// Vaadin lifecycle

    @Override
    public void attach() {
        super.attach();
        this.getContainer().connect();
    }

    @Override
    public void detach() {
        this.getContainer().disconnect();
        super.detach();
    }
}

