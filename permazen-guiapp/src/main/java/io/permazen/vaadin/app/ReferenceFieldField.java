
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin.app;

import com.google.common.base.Preconditions;
import com.vaadin.server.Sizeable;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Component;
import com.vaadin.ui.CustomField;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.HorizontalSplitPanel;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.VerticalLayout;

import io.permazen.CopyState;
import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.core.ObjId;
import io.permazen.parse.ParseSession;
import io.permazen.vaadin.ConfirmWindow;
import io.permazen.vaadin.JObjectChooser;
import io.permazen.vaadin.JObjectContainer;
import io.permazen.vaadin.SizedLabel;
import io.permazen.vaadin.SmallButton;

import org.dellroad.stuff.spring.RetryTransaction;

/**
 * A Vaadin {@link com.vaadin.ui.Field} that edits a database object reference. Supports displaying,
 * but not selecting, a null value; wrap in a {@link io.permazen.vaadin.NullableField} to allow for that.
 */
@SuppressWarnings("serial")
public class ReferenceFieldField extends CustomField<JObject> {

    private final JTransaction dest;
    private final HorizontalLayout layout = new HorizontalLayout();
    private final ParseSession session;
    private final String name;
    private final Class<?> type;
    private final JObjectContainer.RefLabelPropertyDef refLabelPropertyDef = new JObjectContainer.RefLabelPropertyDef();
    private final SmallButton changeButton = new SmallButton("Change...", e -> this.change());

    private Component refLabel = new Label();

    /**
     * Constructor.
     *
     * @param dest target transaction for the chosen {@link JObject}
     * @param session session for expression parsing
     * @param name name of the property
     * @param type type restriction, or null for none
     */
    public ReferenceFieldField(JTransaction dest, ParseSession session, String name, Class<?> type) {

        // Initialize
        Preconditions.checkArgument(dest != null, "null dest");
        Preconditions.checkArgument(session != null, "null session");
        Preconditions.checkArgument(name != null, "null name");
        this.dest = dest;
        this.session = session;
        this.name = name;
        this.type = type;

        // Listen for property value and read-only status changes
        this.addValueChangeListener(e -> this.updateDisplay());
        this.addReadOnlyStatusChangeListener(e -> this.updateDisplay());
    }

// CustomField

    @Override
    public Class<JObject> getType() {
        return JObject.class;
    }

    @Override
    protected HorizontalLayout initContent() {
        this.layout.setMargin(false);
        this.layout.setSpacing(true);
        this.layout.addComponent(this.refLabel);
        this.layout.addComponent(new Label("\u00a0\u00a0"));
        this.layout.addComponent(this.changeButton);
        this.layout.setComponentAlignment(this.changeButton, Alignment.MIDDLE_LEFT);
        this.updateDisplay();
        return this.layout;
    }

    @Override
    protected void setInternalValue(JObject jobj) {
        super.setInternalValue(jobj);
        this.updateDisplay();
    }

// Internal methods

    // Update components to reflect new value
    private void updateDisplay() {
        final JObject jobj = this.getValue();
        final boolean readOnly = this.isReadOnly();
        final Component newLabel = jobj != null ?
          this.refLabelPropertyDef.extract(jobj) : new SizedLabel("<i>Null</i>&#160;", ContentMode.HTML);
        this.layout.replaceComponent(this.refLabel, newLabel);
        this.refLabel = newLabel;
        this.changeButton.setEnabled(!readOnly);
    }

    // Handle change button click
    private void change() {
        this.new ChangeWindow().show();
    }

// ChangeWindow

    public class ChangeWindow extends ConfirmWindow {

        private final JObjectChooser objectChooser;

        ChangeWindow() {
            super(ReferenceFieldField.this.getUI(), "Select " + ReferenceFieldField.this.name);
            this.setWidth(800, Sizeable.Unit.PIXELS);
            this.setHeight(500, Sizeable.Unit.PIXELS);
            this.objectChooser = new JObjectChooser(ReferenceFieldField.this.session, ReferenceFieldField.this.type, false);
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
        @org.springframework.transaction.annotation.Transactional("permazenGuiTransactionManager")
        protected boolean execute() {
            final ObjId id = this.objectChooser.getObjId();
            if (id == null)
                return true;
            final JObject jobj = JTransaction.getCurrent().get(id);
            if (!jobj.exists()) {
                Notification.show("Object " + id + " no longer exists", null, Notification.Type.WARNING_MESSAGE);
                return false;
            }
            try {
                ReferenceFieldField.this.setValue(jobj.copyTo(ReferenceFieldField.this.dest, new CopyState()));
            } catch (Exception e) {
                Notification.show("Error: " + e, null, Notification.Type.ERROR_MESSAGE);
            }
            return true;
        }
    }
}
