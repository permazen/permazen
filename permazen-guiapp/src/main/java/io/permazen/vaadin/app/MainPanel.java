
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin.app;

import com.google.common.base.Preconditions;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.VerticalLayout;

import io.permazen.CopyState;
import io.permazen.JClass;
import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.Permazen;
import io.permazen.UntypedJObject;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.ObjId;
import io.permazen.core.ReferencedObjectException;
import io.permazen.parse.ParseSession;
import io.permazen.vaadin.JObjectChooser;
import io.permazen.vaadin.JObjectContainer;
import io.permazen.vaadin.SizedLabel;

import org.dellroad.stuff.spring.RetryTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

/**
 * Main GUI panel containing the object chooser, object table, buttons, and expresssion text area.
 */
@SuppressWarnings("serial")
public class MainPanel extends VerticalLayout {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Permazen jdb;
    private final ParseSession session;
    private final JObjectChooser objectChooser;

    // Buttons
    private final Button editButton = new Button("Edit", e -> this.editButtonClicked());
    private final Button newButton = new Button("New", e -> this.newButtonClicked());
    private final Button deleteButton = new Button("Delete", e -> this.deleteButtonClicked());
    private final Button upgradeButton = new Button("Upgrade", e -> this.upgradeButtonClicked());
    private final Button refreshButton = new Button("Refresh", e -> this.refreshButtonClicked());

    /**
     * Constructor.
     *
     * @param guiConfig GUI configuration
     */
    public MainPanel(final GUIConfig guiConfig) {
        this(new GUISession(guiConfig));
    }

    /**
     * Constructor.
     *
     * @param session parse session for expression parsing
     */
    public MainPanel(ParseSession session) {
        Preconditions.checkArgument(session != null, "null session");
        this.session = session;
        this.jdb = session.getPermazen();
        Preconditions.checkArgument(this.jdb != null, "session is not a Permazen session");

        // Setup object chooser
        this.objectChooser = new JObjectChooser(this.session, null, true);

        // Listen to object selections
        this.objectChooser.addValueChangeListener(e -> this.selectObject(this.objectChooser.getObjId()));
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

    public ParseSession getParseSession() {
        return this.session;
    }

    public JObjectChooser getJObjectChooser() {
        return this.objectChooser;
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

    private void editButtonClicked() {
        final ObjId id = this.objectChooser.getObjId();
        if (id == null)
            return;
        this.log.info("editing object {}", id);

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
    @Transactional("permazenGuiTransactionManager")
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
        this.log.info("creating new object of type {}", jclass.getType().getName());
        new JObjectEditorWindow(this.getUI(), this.session, jclass).show();
    }

// Delete

    private void deleteButtonClicked() {
        final ObjId id = this.objectChooser.getObjId();
        if (id == null)
            return;
        this.log.info("deleting object {}", id);
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
    @Transactional("permazenGuiTransactionManager")
    private boolean doDelete(ObjId id) {
        final boolean deleted = JTransaction.getCurrent().get(id).delete();
        if (deleted)
            this.objectChooser.getJObjectContainer().reloadAfterCommit();
        return deleted;
    }

// Upgrade

    private void upgradeButtonClicked() {
        final ObjId id = this.objectChooser.getObjId();
        if (id == null)
            return;
        final int newVersion = this.jdb.getActualVersion();
        this.log.info("upgrading object {} to schema version {}", id, newVersion);
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
    @Transactional("permazenGuiTransactionManager")
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
    @Transactional("permazenGuiTransactionManager")
    private boolean canUpgrade(ObjId id) {
        final JObject jobj = JTransaction.getCurrent().get(id);
        return jobj.exists() && jobj.getSchemaVersion() != this.jdb.getActualVersion();
    }

// GUISession

    private static class GUISession extends ParseSession {

        private final GUIConfig guiConfig;

        GUISession(GUIConfig guiConfig) {
            super(guiConfig.getPermazen());
            this.guiConfig = guiConfig;
            this.setReadOnly(this.guiConfig.isReadOnly());
            this.setSchemaModel(this.guiConfig.getPermazen().getSchemaModel());
            this.setSchemaVersion(this.guiConfig.getSchemaVersion());
            this.setAllowNewSchema(this.guiConfig.isAllowNewSchema());
            this.loadFunctionsFromClasspath();
        }

        @Override
        protected void reportException(Exception e) {
            Notification.show("Error: " + e.getMessage(), null, Notification.Type.ERROR_MESSAGE);
            if (this.guiConfig.isVerbose())
                LoggerFactory.getLogger(this.getClass()).info("exception in parse session", e);
        }
    }
}

