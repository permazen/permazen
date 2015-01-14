
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.data.Property;
import com.vaadin.server.Sizeable;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.HorizontalSplitPanel;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.VerticalLayout;

import org.dellroad.stuff.spring.RetryTransaction;
import org.jsimpledb.JObject;
import org.jsimpledb.JTransaction;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjIdSet;
import org.jsimpledb.parse.ParseSession;
import org.springframework.transaction.annotation.Transactional;

/**
 * Edits an object reference.
 */
@SuppressWarnings("serial")
public class ObjectEditor extends HorizontalLayout {

    private final JTransaction dest;
    private final ParseSession session;
    private final String name;
    private final Class<?> type;
    private final Property<JObject> property;
    private final JObjectContainer.RefLabelPropertyDef refLabelPropertyDef = new JObjectContainer.RefLabelPropertyDef();

    private final SmallButton changeButton = new SmallButton("Change...", new Button.ClickListener() {
        @Override
        public void buttonClick(Button.ClickEvent event) {
            ObjectEditor.this.change();
        }
    });
    private final SmallButton nullButton = new SmallButton("Null", new Button.ClickListener() {
        @Override
        public void buttonClick(Button.ClickEvent event) {
            ObjectEditor.this.setNull();
        }
    });

    private Component refLabel = new Label();

    /**
     * Conveinence constructor. Sets {@code allowNull} to true.
     */
    public ObjectEditor(JTransaction dest, ParseSession session, String name, Class<?> type, Property<JObject> property) {
        this(dest, session, name, type, property, true);
    }

    /**
     * Constructor.
     *
     * @param dest destination transaction for the chosen object
     * @param session session for expression parsing
     * @param name name of the property
     * @param type type restriction, or null for none
     * @param property reference property to edit
     * @param allowNull whether null value is allowed
     */
    public ObjectEditor(JTransaction dest, ParseSession session,
      String name, Class<?> type, Property<JObject> property, boolean allowNull) {

        // Initialize
        if (dest == null)
            throw new IllegalArgumentException("null dest");
        if (session == null)
            throw new IllegalArgumentException("null session");
        if (name == null)
            throw new IllegalArgumentException("null name");
        if (property == null)
            throw new IllegalArgumentException("null property");
        this.dest = dest;
        this.session = session;
        this.name = name;
        this.type = type;
        this.property = property;

        // Layout parts
        this.setMargin(false);
        this.setSpacing(true);
        this.addComponent(this.refLabel);
        this.updateRefLabel();
        this.addComponent(new Label("\u00a0\u00a0"));
        this.addComponent(this.changeButton);
        if (allowNull)
            this.addComponent(this.nullButton);

        // Listen for property value and read-only status changes
        if (property instanceof Property.ValueChangeNotifier) {
            ((Property.ValueChangeNotifier)property).addValueChangeListener(new Property.ValueChangeListener() {
                @Override
                public void valueChange(Property.ValueChangeEvent event) {
                    ObjectEditor.this.updateRefLabel();
                }
            });
        }
        if (property instanceof Property.ReadOnlyStatusChangeNotifier) {
            ((Property.ReadOnlyStatusChangeNotifier)property).addReadOnlyStatusChangeListener(
              new Property.ReadOnlyStatusChangeListener() {
                @Override
                public void readOnlyStatusChange(Property.ReadOnlyStatusChangeEvent event) {
                    ObjectEditor.this.updateRefLabel();
                }
            });
        }
    }

    // Update reference label component to reflect new choice
    private void updateRefLabel() {
        final JObject jobj = this.property.getValue();
        final boolean readOnly = this.property.isReadOnly();
        final Component newLabel = jobj != null ?
          this.refLabelPropertyDef.extract(jobj) : new SizedLabel("<i>Null</i>&#160;", ContentMode.HTML);
        this.replaceComponent(this.refLabel, newLabel);
        this.refLabel = newLabel;
        this.changeButton.setEnabled(!readOnly);
        this.nullButton.setEnabled(!readOnly && jobj != null);
    }

    // Handle change button click
    private void change() {
        this.new ChangeWindow().show();
    }

    // Handle null button click
    private void setNull() {
        this.property.setValue(null);
    }

// ChangeWindow

    public class ChangeWindow extends ConfirmWindow {

        private final ObjectChooser objectChooser;

        ChangeWindow() {
            super(ObjectEditor.this.getUI(), "Select " + ObjectEditor.this.name);
            this.setWidth(800, Sizeable.Unit.PIXELS);
            this.setHeight(500, Sizeable.Unit.PIXELS);
            this.objectChooser = new ObjectChooser(ObjectEditor.this.dest.getJSimpleDB(),
              ObjectEditor.this.session, ObjectEditor.this.type, false);
        }

        @Override
        protected void addContent(VerticalLayout layout) {
            final HorizontalSplitPanel objectPanel = this.objectChooser.getObjectPanel();
            objectPanel.setHeight(200, Sizeable.Unit.PIXELS);
            objectPanel.setSplitPosition(40);
            layout.addComponent(objectPanel);
            layout.addComponent(this.objectChooser.getShowForm());
        }

        @Override
        @RetryTransaction
        @Transactional("jsimpledbGuiTransactionManager")
        protected boolean execute() {
            final ObjId id = this.objectChooser.getObjId();
            if (id == null)
                return true;
            final JObject jobj = JTransaction.getCurrent().getJObject(id);
            if (!jobj.exists()) {
                Notification.show("Object " + id + " no longer exists", null, Notification.Type.WARNING_MESSAGE);
                return false;
            }
            try {
                ObjectEditor.this.property.setValue(jobj.copyTo(ObjectEditor.this.dest, id, new ObjIdSet()));
            } catch (Exception e) {
                Notification.show("Error: " + e, null, Notification.Type.ERROR_MESSAGE);
            }
            return true;
        }
    }
}

