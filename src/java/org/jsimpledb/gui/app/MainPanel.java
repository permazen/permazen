
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.gui.app;

import com.vaadin.data.Property;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.VerticalLayout;

import org.dellroad.stuff.spring.RetryTransaction;
import org.jsimpledb.CopyState;
import org.jsimpledb.JClass;
import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JTransaction;
import org.jsimpledb.UntypedJObject;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ReferencedObjectException;
import org.jsimpledb.gui.JObjectChooser;
import org.jsimpledb.gui.JObjectContainer;
import org.jsimpledb.gui.SizedLabel;
import org.jsimpledb.parse.ParseSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

/**
 * Main GUI panel containing the object chooser, object table, buttons, and expresssion text area.
 */
@SuppressWarnings("serial")
public class MainPanel extends VerticalLayout {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    // Buttons
    private final Button editButton = new Button("Edit", new Button.ClickListener() {
        @Override
        public void buttonClick(Button.ClickEvent event) {
            final ObjId id = MainPanel.this.objectChooser.getObjId();
            if (id != null)
                MainPanel.this.editButtonClicked(id);
        }
    });
    private final Button newButton = new Button("New", new Button.ClickListener() {
        @Override
        public void buttonClick(Button.ClickEvent event) {
            MainPanel.this.newButtonClicked();
        }
    });
    private final Button deleteButton = new Button("Delete", new Button.ClickListener() {
        @Override
        public void buttonClick(Button.ClickEvent event) {
            final ObjId id = MainPanel.this.objectChooser.getObjId();
            if (id != null)
                MainPanel.this.deleteButtonClicked(id);
        }
    });
    private final Button upgradeButton = new Button("Upgrade", new Button.ClickListener() {
        @Override
        public void buttonClick(Button.ClickEvent event) {
            final ObjId id = MainPanel.this.objectChooser.getObjId();
            if (id != null)
                MainPanel.this.upgradeButtonClicked(id);
        }
    });
    private final Button refreshButton = new Button("Refresh", new Button.ClickListener() {
        @Override
        public void buttonClick(Button.ClickEvent event) {
            MainPanel.this.refreshButtonClicked();
        }
    });

    private final GUIConfig guiConfig;
    private final JSimpleDB jdb;
    private final ParseSession session;
    private final JObjectChooser objectChooser;

    public MainPanel(GUIConfig guiConfig) {
        this.guiConfig = guiConfig;
        this.jdb = this.guiConfig.getJSimpleDB();

        // Setup parse session
        this.session = new ParseSession(this.jdb) {
            @Override
            protected void reportException(Exception e) {
                Notification.show("Error: " + e.getMessage(), null, Notification.Type.ERROR_MESSAGE);
                if (MainPanel.this.guiConfig.isVerbose())
                    MainPanel.this.log.info("exception in parse session", e);
            }
        };
        this.session.setReadOnly(this.guiConfig.isReadOnly());
        this.session.setSchemaModel(this.jdb.getSchemaModel());
        this.session.setSchemaVersion(this.guiConfig.getSchemaVersion());
        this.session.setAllowNewSchema(this.guiConfig.isAllowNewSchema());

        // Register built-in functions
        this.session.registerStandardFunctions();

        // Register custom functions
        final Iterable<? extends Class<?>> customFunctionClasses = this.guiConfig.getFunctionClasses();
        if (customFunctionClasses != null) {
            for (Class<?> cl : customFunctionClasses) {
                try {
                    this.session.registerFunction(cl);
                } catch (IllegalArgumentException e) {
                    this.log.warn("failed to register function " + cl + ": " + e.getMessage());
                }
            }
        }

        // Setup object chooser
        this.objectChooser = new JObjectChooser(this.session, null, true);

        // Listen to object selections
        this.objectChooser.addValueChangeListener(new Property.ValueChangeListener() {
            @Override
            public void valueChange(Property.ValueChangeEvent event) {
                MainPanel.this.selectObject(MainPanel.this.objectChooser.getObjId());
            }
        });
    }

    @Override
    public void attach() {
        super.attach();
        this.setMargin(false);
        this.setSpacing(true);
        this.setHeight("100%");

        // Layout top split panel
        this.addComponent(this.objectChooser.getObjectPanel());

        // Add row with schema version and buttons
        final HorizontalLayout buttonRow = new HorizontalLayout();
        buttonRow.setSpacing(true);
        buttonRow.setWidth("100%");
        final SizedLabel versionLabel = new SizedLabel("Schema Version " + this.jdb.getActualVersion());
        buttonRow.addComponent(versionLabel);
        buttonRow.setComponentAlignment(versionLabel, Alignment.MIDDLE_LEFT);
        final Label spacer1 = new Label();
        buttonRow.addComponent(spacer1);
        buttonRow.setExpandRatio(spacer1, 1.0f);
        buttonRow.addComponent(this.editButton);
        buttonRow.addComponent(this.newButton);
        buttonRow.addComponent(this.deleteButton);
        buttonRow.addComponent(this.upgradeButton);
        buttonRow.addComponent(this.refreshButton);
        this.addComponent(buttonRow);
        this.setComponentAlignment(buttonRow, Alignment.TOP_RIGHT);

        // Add show form
        this.addComponent(this.objectChooser.getShowForm());

        // Add space filler
        final Label spacer2 = new Label();
        this.addComponent(spacer2);
        this.setExpandRatio(spacer2, 1.0f);

        // Populate table
        //this.selectType(Object.class, true);
    }

// GUI Updates

    // Invoked when an object is clicked on
    protected void selectObject(ObjId id) {

        // New button
        this.newButton.setEnabled(this.objectChooser.getJClass() != null);

        // Update buttons
        if (id == null) {
            this.editButton.setEnabled(false);
            this.deleteButton.setEnabled(false);
            this.upgradeButton.setEnabled(false);
        } else {
            this.editButton.setEnabled(true);
            this.deleteButton.setEnabled(true);
            this.upgradeButton.setEnabled(this.canUpgrade(id));
        }
    }

// Refresh

    private void refreshButtonClicked() {
        this.objectChooser.getJObjectContainer().reload();
    }

// Edit

    private void editButtonClicked(ObjId id) {
        this.log.info("editing object " + id);

        // Copy object
        final JObject jobj = this.doCopyForEdit(id);
        if (jobj == null) {
            Notification.show("Object " + id + " no longer exists", null, Notification.Type.WARNING_MESSAGE);
            return;
        }

        // Ensure object type is known
        if (jobj instanceof UntypedJObject) {
            Notification.show("Can't edit object " + id + " having unknown object type",
              "Storage ID " + id.getStorageId() + " not defined in the current schema version",
              Notification.Type.WARNING_MESSAGE);
            return;
        }

        // Build title component from reference label
        Object refLabel = MainPanel.this.objectChooser.getJObjectContainer().getContainerProperty(
          id, JObjectContainer.REFERENCE_LABEL_PROPERTY).getValue();
        final Component titleComponent = refLabel instanceof Component ? (Component)refLabel : new Label("" + refLabel);

        // Open window
        final JObjectEditorWindow editor = new JObjectEditorWindow(this.getUI(),
          this.session, this.jdb.getJClass(id), jobj, titleComponent);
        editor.setReloadContainerAfterCommit(this.objectChooser.getJObjectContainer());
        editor.show();
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    private JObject doCopyForEdit(ObjId id) {

        // Find object
        final JTransaction jtx = JTransaction.getCurrent();
        final JObject jobj = jtx.get(id);
        if (!jobj.exists())
            return null;

        // Copy out object and its dependencies
        return this.objectChooser.getJObjectContainer().copyWithRelated(jobj, jtx.getSnapshotTransaction(), new CopyState());
    }

// New

    private void newButtonClicked() {
        final JClass<?> jclass = this.objectChooser.getJClass();
        if (jclass == null) {
            Notification.show("Can't create object having unknown type",
              "Please select an object type first", Notification.Type.WARNING_MESSAGE);
            return;
        }
        this.log.info("creating new object of type " + jclass.getType().getName());
        new JObjectEditorWindow(this.getUI(), this.session, jclass).show();
    }

// Delete

    private void deleteButtonClicked(ObjId id) {
        this.log.info("deleting object " + id);
        final boolean deleted;
        try {
            deleted = this.doDelete(id);
        } catch (DeletedObjectException e) {
            Notification.show("Object " + e.getId() + " no longer exists", null, Notification.Type.WARNING_MESSAGE);
            return;
        } catch (ReferencedObjectException e) {
            Notification.show("Object " + id + " is referenced by " + e.getReferrer(),
              e.getMessage(), Notification.Type.ERROR_MESSAGE);
            return;
        }
        if (deleted)
            Notification.show("Removed object " + id);
        else
            Notification.show("Could not delete object " + id, null, Notification.Type.WARNING_MESSAGE);
        this.selectObject(null);
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    private boolean doDelete(ObjId id) {
        final boolean deleted = JTransaction.getCurrent().get(id).delete();
        if (deleted)
            this.objectChooser.getJObjectContainer().reloadAfterCommit();
        return deleted;
    }

// Upgrade

    private void upgradeButtonClicked(ObjId id) {
        final int newVersion = this.jdb.getActualVersion();
        this.log.info("upgrading object " + id + " to schema version " + newVersion);
        final int oldVersion = this.doUpgrade(id);
        switch (oldVersion) {
        case -1:
            Notification.show("Object " + id + " no longer exists", null, Notification.Type.WARNING_MESSAGE);
            break;
        case 0:
            Notification.show("Object " + id + " was already upgraded", null, Notification.Type.WARNING_MESSAGE);
            break;
        default:
            Notification.show("Upgraded object " + id + " version from " + oldVersion + " to " + newVersion);
            break;
        }
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    private int doUpgrade(ObjId id) {
        final JObject jobj = JTransaction.getCurrent().get(id);
        final int oldVersion;
        try {
            oldVersion = jobj.getSchemaVersion();
        } catch (DeletedObjectException e) {
            return -1;
        }
        final boolean upgraded = jobj.upgrade();
        if (upgraded)
            this.objectChooser.getJObjectContainer().reloadAfterCommit();
        return upgraded ? oldVersion : 0;
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    private boolean canUpgrade(ObjId id) {
        final JObject jobj = JTransaction.getCurrent().get(id);
        return jobj.exists() && jobj.getSchemaVersion() != this.jdb.getActualVersion();
    }
}

