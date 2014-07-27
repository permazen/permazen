
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.data.Property;
import com.vaadin.data.util.MethodProperty;
import com.vaadin.server.Sizeable;
import com.vaadin.server.VaadinSession;
import com.vaadin.shared.ui.datefield.Resolution;
import com.vaadin.ui.AbstractField;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Component;
import com.vaadin.ui.DefaultFieldFactory;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.PopupDateField;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;

import javax.validation.constraints.NotNull;

import org.dellroad.stuff.spring.RetryTransaction;
import org.dellroad.stuff.vaadin7.EnumComboBox;
import org.dellroad.stuff.vaadin7.FieldBuilder;
import org.dellroad.stuff.vaadin7.VaadinConfigurable;
import org.dellroad.stuff.vaadin7.VaadinUtil;
import org.jsimpledb.JClass;
import org.jsimpledb.JField;
import org.jsimpledb.JObject;
import org.jsimpledb.JReferenceField;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JSimpleField;
import org.jsimpledb.JTransaction;
import org.jsimpledb.change.ObjectCreate;
import org.jsimpledb.change.ObjectDelete;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ReferencedObjectException;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.parse.ParseSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

/**
 * Main GUI panel containing the various tabs.
 */
@SuppressWarnings("serial")
@VaadinConfigurable
public class MainPanel extends VerticalLayout {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    // Buttons
    private final Button editButton = new Button("Edit", new Button.ClickListener() {
        @Override
        public void buttonClick(Button.ClickEvent event) {
            MainPanel.this.editButtonClicked(MainPanel.this.objectChooser.getObjId());
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
            MainPanel.this.deleteButtonClicked(MainPanel.this.objectChooser.getObjId());
        }
    });
    private final Button upgradeButton = new Button("Upgrade", new Button.ClickListener() {
        @Override
        public void buttonClick(Button.ClickEvent event) {
            MainPanel.this.upgradeButtonClicked(MainPanel.this.objectChooser.getObjId());
        }
    });

    private final GUIConfig guiConfig;
    private final JSimpleDB jdb;
    private final ParseSession session;
    private final ObjectChooser objectChooser;

    @Autowired(required = false)
    private ChangePublisher changePublisher;

    public MainPanel(GUIConfig guiConfig) {
        this.guiConfig = guiConfig;
        this.jdb = this.guiConfig.getJSimpleDB();

        // Setup parse session
        this.session = new ParseSession(this.jdb) {
            @Override
            protected void reportException(Exception e) {
                Notification.show(e.getMessage(), null, Notification.Type.ERROR_MESSAGE);
            }
        };
        this.session.setReadOnly(this.guiConfig.isReadOnly());
        this.session.setSchemaModel(this.jdb.getSchemaModel());
        this.session.setSchemaVersion(this.guiConfig.getSchemaVersion());
        this.session.setAllowNewSchema(this.guiConfig.isAllowNewSchema());
        for (Class<?> cl : this.guiConfig.getFunctionClasses()) {
            try {
                this.session.registerFunction(cl);
            } catch (IllegalArgumentException e) {
                this.log.warn("failed to register function " + cl + ": " + e.getMessage());
            }
        }

        // Setup object chooser
        this.objectChooser = new ObjectChooser(this.jdb, this.session, null, true);

        // Listen to object selections
        this.objectChooser.getObjectTable().addValueChangeListener(new Property.ValueChangeListener() {
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
        final SizedLabel versionLabel = new SizedLabel("Schema Version " + this.jdb.getLastVersion());
        buttonRow.addComponent(versionLabel);
        buttonRow.setComponentAlignment(versionLabel, Alignment.MIDDLE_LEFT);
        final Label spacer1 = new Label();
        buttonRow.addComponent(spacer1);
        buttonRow.setExpandRatio(spacer1, 1.0f);
        buttonRow.addComponent(this.editButton);
        buttonRow.addComponent(this.newButton);
        buttonRow.addComponent(this.deleteButton);
        buttonRow.addComponent(this.upgradeButton);
        this.addComponent(buttonRow);
        this.setComponentAlignment(buttonRow, Alignment.TOP_RIGHT);

        // Add show form
        this.addComponent(this.objectChooser.getShowForm());

        // Add space filler
        final Label spacer2 = new Label();
        this.addComponent(spacer2);
        this.setExpandRatio(spacer2, 1.0f);

        // Populate table
        //this.selectType(TypeToken.of(Object.class), true);
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

// Edit

    private void editButtonClicked(ObjId id) {
        this.log.info("editing object " + id);

        // Copy object
        final JObject jobj = this.doCopyForEdit(id);
        if (jobj == null) {
            Notification.show("Object " + id + " no longer exists", null, Notification.Type.WARNING_MESSAGE);
            return;
        }

        // Ensure type is known
        final int storageId = id.getStorageId();
        final JClass<?> jclass;
        try {
            jclass = this.jdb.getJClass(storageId);
        } catch (IllegalArgumentException e) {
            Notification.show("Can't edit object " + id + " having unknown type",
              "The type with storage ID " + storageId + " does not exist in schema version " + this.jdb.getLastVersion(),
              Notification.Type.WARNING_MESSAGE);
            return;
        }

        // Open window
        new EditWindow(jobj, jclass, false).show();
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    private JObject doCopyForEdit(ObjId id) {
        final JObject jobj = JTransaction.getCurrent().getJObject(id);
        if (!jobj.exists())
            return null;
        return jobj.copyOut();
    }

// New

    private void newButtonClicked() {
        final JClass<?> jclass = this.objectChooser.getJClass();
        if (jclass == null) {
            Notification.show("Can't create object having unknown type",
              "Please select an object type first", Notification.Type.WARNING_MESSAGE);
            return;
        }
        this.log.info("creating new object of type " + jclass.getTypeToken());
        new EditWindow(this.doCreateForEdit(jclass), jclass, true).show();
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    private JObject doCreateForEdit(JClass<?> jclass) {
        return (JObject)JTransaction.getCurrent().getSnapshotTransaction().create(jclass);
    }

// Delete

    private void deleteButtonClicked(ObjId id) {
        this.log.info("deleting object " + id);
        try {
            this.doDelete(id);
        } catch (DeletedObjectException e) {
            Notification.show("Object " + id + " no longer exists", null, Notification.Type.WARNING_MESSAGE);
        } catch (ReferencedObjectException e) {
            Notification.show("Object " + id + " is referenced by " + e.getReferrer(),
              e.getMessage(), Notification.Type.ERROR_MESSAGE);
        }
        Notification.show("Removed object " + id);
        this.selectObject(null);
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    private boolean doDelete(ObjId id) {
        final JObject jobj = JTransaction.getCurrent().getJObject(id);
        final boolean deleted = jobj.delete();
        if (deleted && this.changePublisher != null)
            this.changePublisher.publishChangeOnCommit(new ObjectDelete<Object>(jobj));
        return deleted;
    }

// Upgrade

    private void upgradeButtonClicked(ObjId id) {
        final int newVersion = this.jdb.getLastVersion();
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
        final JObject jobj = JTransaction.getCurrent().getJObject(id);
        final int oldVersion;
        try {
            oldVersion = jobj.getSchemaVersion();
        } catch (DeletedObjectException e) {
            return -1;
        }
        final boolean upgraded = jobj.upgrade();
        if (upgraded && this.changePublisher != null)
            this.changePublisher.publishChangeOnCommit(jobj);
        return upgraded ? oldVersion : 0;
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    private boolean canUpgrade(ObjId id) {
        final JObject jobj = JTransaction.getCurrent().getJObject(id);
        return jobj.exists() && jobj.getSchemaVersion() != this.jdb.getLastVersion();
    }

// EditWindow

    public class EditWindow extends ConfirmWindow {

        private final JObject jobj;
        private final JClass<?> jclass;
        private final boolean create;
        private final LinkedHashMap<String, Component> editorMap = new LinkedHashMap<>();

        EditWindow(JObject jobj, JClass<?> jclass, boolean create) {
            super(MainPanel.this.getUI(), (create ? "New" : "Edit") + " " + jclass.getName());
            this.setWidth(600, Sizeable.Unit.PIXELS);
            this.setHeight(450, Sizeable.Unit.PIXELS);
            this.jobj = jobj;
            this.jclass = jclass;
            this.create = create;

            // Get the names of all database properties
            final SortedMap<String, JField> fieldMap = jclass.getJFieldsByName();

            // First introspect for any @FieldBuilder.* annotations
            final Map<String, AbstractField<?>> fieldBuilderFields = new FieldBuilder().buildBeanPropertyFields(this.jobj);
            for (Map.Entry<String, AbstractField<?>> entry : fieldBuilderFields.entrySet()) {
                final String fieldName = entry.getKey();
                if (!fieldMap.keySet().contains(fieldName))
                    continue;
                final AbstractField<?> field = entry.getValue();
                this.editorMap.put(fieldName, this.buildFieldFieldEditor(fieldName, field));
            }

            // Now build editors for the remaining properties
            for (Map.Entry<String, JField> entry : fieldMap.entrySet()) {
                final String fieldName = entry.getKey();
                if (this.editorMap.containsKey(fieldName))
                    continue;
                final JField jfield = entry.getValue();
                final Component editor;
                if (jfield instanceof JSimpleField)
                    editor = this.buildSimpleFieldEditor((JSimpleField)jfield);
                else
                    editor = new Label("TODO: " + jfield);       // TODO
                this.editorMap.put(fieldName, editor);
            }
        }

        @Override
        protected void addContent(VerticalLayout layout) {
            if (!this.create) {
                layout.addComponent((Component)MainPanel.this.objectChooser.getObjectContainer().getContainerProperty(
                  this.jobj.getObjId(), JObjectContainer.REFERENCE_LABEL_PROPERTY).getValue());
            }
            final FormLayout formLayout = new FormLayout();
            for (Component component : this.editorMap.values())
                formLayout.addComponent(component);
            layout.addComponent(formLayout);
        }

        @Override
        @RetryTransaction
        @Transactional("jsimpledbGuiTransactionManager")
        protected boolean execute() {
            final JTransaction jtx = JTransaction.getCurrent();

            // Find/create object
            final ObjId id = this.jobj.getObjId();
            final JObject target = this.create ? (JObject)jtx.create(this.jclass) : jtx.getJObject(id);

            // Verify object still exists when editing
            if (!this.create && !target.exists()) {
                Notification.show("Object " + id + " no longer exists", null, Notification.Type.WARNING_MESSAGE);
                return true;
            }

            // Copy fields
            this.jobj.copyTo(jtx, id);

            // Broadcast update event after successful commit
            if (MainPanel.this.changePublisher != null) {
                if (create)
                    MainPanel.this.changePublisher.publishChangeOnCommit(new ObjectCreate<Object>(target));
                else
                    MainPanel.this.changePublisher.publishChangeOnCommit(target);
            }

            // Show notification after successful commit
            final VaadinSession vaadinSession = VaadinUtil.getCurrentSession();
            jtx.getTransaction().addCallback(new Transaction.CallbackAdapter() {
                @Override
                public void afterCommit() {
                    VaadinUtil.invoke(vaadinSession, new Runnable() {
                        @Override
                        public void run() {
                            Notification.show((EditWindow.this.create ? "Created" : "Updated") + " object " + id);
                        }
                    });
                }
            });
            return true;
        }

        protected Component buildFieldFieldEditor(String fieldName, AbstractField<?> field) {
            return field;
        }

        protected Component buildSimpleFieldEditor(JSimpleField jfield) {

            // Get field info
            final boolean allowNull = jfield.getGetter().getAnnotation(NotNull.class) == null;

            // Get the property we want to edit
            final Property<?> property = this.buildProperty(jfield);

            // Build editor
            final Component editor = this.buildSimpleFieldEditor(jfield, property, allowNull);
            editor.setCaption(this.buildCaption(jfield.getName()));
            return editor;
        }

        @SuppressWarnings("unchecked")
        protected Component buildSimpleFieldEditor(JSimpleField jfield, Property<?> property, boolean allowNull) {

            // Get property type
            final Class<?> propertyType = jfield.getGetter().getReturnType();

            // Use object choosers for references
            if (jfield instanceof JReferenceField) {
                final JReferenceField refField = (JReferenceField)jfield;
                final Method getter = refField.getGetter();
                final Method setter = refField.getSetter();
                final ObjectEditor objectEditor = new ObjectEditor(this.jobj.getTransaction(), MainPanel.this.session,
                  getter.getReturnType(), new MethodProperty<JObject>(JObject.class, this.jobj, getter, setter), allowNull);
                return objectEditor;
            }

            // Use ComboBox for Enum's
            if (Enum.class.isAssignableFrom(propertyType)) {
                final EnumComboBox comboBox = this.createEnumComboBox(propertyType.asSubclass(Enum.class), allowNull);
                comboBox.setPropertyDataSource(property);
                if (allowNull)
                    comboBox.setInputPrompt("Null");
                return comboBox;
            }

            // Use DatePicker for dates
            if (Date.class.isAssignableFrom(propertyType)) {
                final PopupDateField dateField = new PopupDateField();
                dateField.setImmediate(true);
                dateField.setResolution(Resolution.SECOND);
                dateField.setPropertyDataSource(property);
                return dateField;
            }

            // Use text field for all other field types
            final TextField textField = new TextField();
            textField.setWidth("100%");
            if (allowNull)
                textField.setNullRepresentation("");
            textField.setPropertyDataSource(property);
            final FieldType<?> fieldType = this.getFieldType(jfield);
            if (fieldType.getTypeToken().getRawType() != String.class)
                textField.setConverter(this.buildSimpleFieldConverter(fieldType));
            return textField;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private Property<?> buildProperty(JSimpleField jfield) {
            return new MethodProperty(jfield.getGetter().getReturnType(), this.jobj, jfield.getGetter(), jfield.getSetter());
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private <T extends Enum> EnumComboBox createEnumComboBox(Class<T> enumType, boolean allowNull) {
            return new EnumComboBox(enumType, allowNull);
        }

        private <T> SimpleFieldConverter<T> buildSimpleFieldConverter(FieldType<T> fieldType) {
            return new SimpleFieldConverter<T>(fieldType);
        }

        private FieldType<?> getFieldType(JSimpleField jfield) {
            return MainPanel.this.jdb.getDatabase().getFieldTypeRegistry().getFieldType(jfield.getTypeName());
        }

        private String buildCaption(String fieldName) {
            return DefaultFieldFactory.createCaptionByPropertyId(fieldName) + ":";
        }
    }
}

