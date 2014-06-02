
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.ui.Notification;
import com.vaadin.ui.Table;

import java.util.ArrayList;
import java.util.List;

import org.jsimpledb.JClass;
import org.jsimpledb.JObject;
import org.jsimpledb.JTransaction;

@SuppressWarnings("serial")
public class ObjectTable extends Table implements ActionListBuilder<JObject> {

    private final MainPanel mainPanel;
    private final JClass<?> jclass;

    private final ArrayList<String> columnIds = new ArrayList<>();

    public ObjectTable(MainPanel mainPanel, JClass<?> jclass) {
        super(jclass.getName(), new ObjectContainer(jclass));
        this.mainPanel = mainPanel;
        this.jclass = jclass;

        this.setSelectable(false);
        this.setImmediate(false);
        this.setSizeFull();

        this.addColumn(ObjectContainer.OBJ_ID_PROPERTY, "ID", 120, Table.Align.CENTER);
        this.addColumn(ObjectContainer.TYPE_PROPERTY, "Type", 80, Table.Align.CENTER);
        this.addColumn(ObjectContainer.VERSION_PROPERTY, "Version", 50, Table.Align.CENTER);
        for (String fieldName : this.getContainer().getJClass().getJFieldsByName().keySet()) {
            this.addColumn(fieldName, fieldName, 120, Table.Align.CENTER);
            this.setColumnExpandRatio(fieldName, 1.0f);
        }

        this.setColumnCollapsingAllowed(true);
        this.setColumnCollapsed(ObjectContainer.VERSION_PROPERTY, true);

        this.setVisibleColumns(this.columnIds.toArray());

        // Add actions
        this.addActionHandler(new DefaultActionHandler<JObject>(this.getContainer(), this));
    }

    protected void addColumn(String property, String name, int width, Table.Align alignment) {
        this.setColumnHeader(property, name);
        this.setColumnWidth(property, width);
        if (alignment != null)
            this.setColumnAlignment(property, alignment);
        this.columnIds.add(property);
    }

    protected ObjectContainer getContainer() {
        return (ObjectContainer)this.getContainerDataSource();
    }

// ActionListBuilder

    @Override
    public List<? extends Action> buildActionList(JObject target) {
        if (target == null)
            return null;
        final ArrayList<Action> list = new ArrayList<>();
        list.add(new DeleteAction(target));
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

    private class DeleteAction extends Action {

        final JObject jobj;

        DeleteAction(JObject jobj) {
            super("Delete", true);
            this.jobj = jobj;
        }

        @Override
        protected void performAction() {
            final JObject actual = JTransaction.getCurrent().getJObject(this.jobj.getObjId());
            final boolean deleted = actual.delete();
            if (!deleted)
                Notification.show("Object does not exist", "Apparently it was already deleted.", Notification.Type.WARNING_MESSAGE);
            else
                Notification.show("Object " + this.jobj.getObjId() + " deleted.");
        }
    }
}

