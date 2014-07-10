
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.ui.Table;
import com.vaadin.ui.TreeTable;

import java.util.ArrayList;
import java.util.List;

import org.jsimpledb.JClass;

/**
 * Table showing object types.
 */
@SuppressWarnings("serial")
public class TypeTable extends TreeTable implements ActionListBuilder<JClassContainer.Node> {

    private final MainPanel mainPanel;

    public TypeTable(MainPanel mainPanel) {
        super("Object Types", new JClassContainer());
        this.mainPanel = mainPanel;

        this.setSelectable(false);
        this.setImmediate(false);
        this.setSizeFull();
        this.setHierarchyColumn("name");
        this.setAnimationsEnabled(true);

        this.addColumn(JClassContainer.Node.NAME_PROPERTY, "Name", 140, Table.Align.LEFT);
        this.addColumn(JClassContainer.Node.STORAGE_ID_PROPERTY, "SID", 40, Table.Align.CENTER);
        this.addColumn(JClassContainer.Node.TYPE_PROPERTY, "Type", 250, Table.Align.CENTER);

        this.setColumnCollapsingAllowed(true);
        this.setVisibleColumns(JClassContainer.Node.NAME_PROPERTY,
          JClassContainer.Node.STORAGE_ID_PROPERTY, JClassContainer.Node.TYPE_PROPERTY);

        // Add actions
        this.addActionHandler(new DefaultActionHandler<JClassContainer.Node>(this.getContainer(), this));
    }

    protected void addColumn(String property, String name, int width, Table.Align alignment) {
        this.setColumnHeader(property, name);
        this.setColumnWidth(property, width);
        if (alignment != null)
            this.setColumnAlignment(property, alignment);
    }

    protected JClassContainer getContainer() {
        return (JClassContainer)this.getContainerDataSource();
    }

// ActionListBuilder

    @Override
    public List<? extends Action> buildActionList(JClassContainer.Node target) {
        if (target == null)
            return null;
        final ArrayList<Action> list = new ArrayList<>();
        list.add(new ShowAllObjectsAction(target.getJClass()));
        return list;
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

// Actions

    private class ShowAllObjectsAction extends Action {

        final JClass<?> jclass;

        ShowAllObjectsAction(JClass<?> jclass) {
            super("Show all", true);
            this.jclass = jclass;
        }

        @Override
        protected void performAction() {
            TypeTable.this.mainPanel.addTab(new ObjectPanel(TypeTable.this.mainPanel, this.jclass), this.jclass.getName());
        }
    }
}

