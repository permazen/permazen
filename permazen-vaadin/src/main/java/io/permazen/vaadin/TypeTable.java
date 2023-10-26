
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.vaadin.ui.Table;
import com.vaadin.ui.TreeTable;

/**
 * Table showing object types.
 */
@SuppressWarnings("serial")
public class TypeTable extends TreeTable {

    public TypeTable(TypeContainer container) {
        super(null, container);

        this.setSelectable(true);
        this.setImmediate(true);
        this.setSizeFull();
        this.setHierarchyColumn(TypeContainer.NAME_PROPERTY);
        this.setAnimationsEnabled(true);

        this.addColumn(TypeContainer.NAME_PROPERTY, "Type", 140, Table.Align.LEFT);
        this.addColumn(TypeContainer.STORAGE_ID_PROPERTY, "SID", 40, Table.Align.CENTER);
        this.addColumn(TypeContainer.TYPE_PROPERTY, "Java Type", 250, Table.Align.CENTER);

        this.setColumnExpandRatio(TypeContainer.NAME_PROPERTY, 1.0f);

        this.setVisibleColumns(TypeContainer.NAME_PROPERTY, TypeContainer.STORAGE_ID_PROPERTY, TypeContainer.TYPE_PROPERTY);
        this.setColumnCollapsingAllowed(true);
        this.setColumnCollapsed(TypeContainer.STORAGE_ID_PROPERTY, true);
        this.setColumnCollapsed(TypeContainer.TYPE_PROPERTY, true);
    }

    protected void addColumn(String property, String name, int width, Table.Align alignment) {
        this.setColumnHeader(property, name);
        this.setColumnWidth(property, width);
        if (alignment != null)
            this.setColumnAlignment(property, alignment);
    }

    public TypeContainer getContainer() {
        return (TypeContainer)this.getContainerDataSource();
    }

// Vaadin lifecycle

    @Override
    public void attach() {
        super.attach();
        this.getContainer().connect();

        // Expand all root nodes
        for (Class<?> type : this.getContainer().rootItemIds())
            this.setCollapsed(type, false);
    }

    @Override
    public void detach() {
        this.getContainer().disconnect();
        super.detach();
    }
}
