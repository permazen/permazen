
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.vaadin.data.Property;
import com.vaadin.data.fieldgroup.FieldGroup;
import com.vaadin.data.util.BeanItem;
import com.vaadin.data.util.MethodProperty;
import com.vaadin.server.Sizeable;
import com.vaadin.server.VaadinSession;
import com.vaadin.shared.ui.datefield.Resolution;
import com.vaadin.ui.AbstractField;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.DefaultFieldFactory;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.PopupDateField;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;

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
import org.jsimpledb.JCollectionField;
import org.jsimpledb.JField;
import org.jsimpledb.JFieldSwitchAdapter;
import org.jsimpledb.JMapField;
import org.jsimpledb.JObject;
import org.jsimpledb.JReferenceField;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JSimpleField;
import org.jsimpledb.JTransaction;
import org.jsimpledb.ObjIdSet;
import org.jsimpledb.ValidationException;
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
import org.springframework.transaction.UnexpectedRollbackException;
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
                Notification.show("Error: " + e.getMessage(), null, Notification.Type.ERROR_MESSAGE);
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
              e.getMessage(), Notification.Type.WARNING_MESSAGE);
            return;
        }

        // Open window
        new EditWindow(jobj, jclass, false).show();
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    private JObject doCopyForEdit(ObjId id) {

        // Get object
        final JObject jobj = JTransaction.getCurrent().getJObject(id);
        if (!jobj.exists())
            return null;

        // Copy object and dependencies
        final ObjIdSet idSet = new ObjIdSet();
        final JObject copy = this.objectChooser.getObjectContainer().copyOut(jobj, idSet);

        // Copy out all objects this object refers to, so we can display their reference labels
        for (JField jfield : this.jdb.getJClass(id).getJFieldsByStorageId().values()) {
            jfield.visit(new JFieldSwitchAdapter<Void>() {

                @Override
                public Void caseJReferenceField(JReferenceField field) {
                    MainPanel.this.objectChooser.getObjectContainer().copyOut(
                      (JObject)field.getValue(JTransaction.getCurrent(), jobj), idSet);
                    return null;
                }

                @Override
                public Void caseJMapField(JMapField field) {
                    if (field.getKeyField() instanceof JReferenceField || field.getValueField() instanceof JReferenceField) {
                        for (Map.Entry<?, ?> entry : field.getValue(JTransaction.getCurrent(), jobj).entrySet()) {
                            final Object key = entry.getKey();
                            if (key instanceof JObject)
                                MainPanel.this.objectChooser.getObjectContainer().copyOut((JObject)key, idSet);
                            final Object value = entry.getKey();
                            if (value instanceof JObject)
                                MainPanel.this.objectChooser.getObjectContainer().copyOut((JObject)value, idSet);
                        }
                    }
                    return null;
                }

                @Override
                protected Void caseJCollectionField(JCollectionField field) {
                    if (field.getElementField() instanceof JReferenceField) {
                        for (Object elem : field.getValue(JTransaction.getCurrent(), jobj))
                            MainPanel.this.objectChooser.getObjectContainer().copyOut((JObject)elem, idSet);
                    }
                    return null;
                }

                @Override
                protected Void caseJField(JField field) {
                    return null;
                }
            });
        }

        // Done
        return copy;
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
        final JObject jobj = JTransaction.getCurrent().getJObject(id);
        final boolean deleted = jobj.delete();
        if (deleted && this.changePublisher != null)
            this.changePublisher.publishChangeOnCommit(new ObjectDelete<Object>(jobj));
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
        return jobj.exists() && jobj.getSchemaVersion() != this.jdb.getActualVersion();
    }

// EditWindow

    public class EditWindow extends ConfirmWindow {

        private final JObject jobj;
        private final JClass<?> jclass;
        private final boolean create;
        private final FieldGroup fieldGroup = new FieldGroup();
        private final LinkedHashMap<String, Editor> editorMap = new LinkedHashMap<>();

        EditWindow(JObject jobj, JClass<?> jclass, boolean create) {
            super(MainPanel.this.getUI(), (create ? "New" : "Edit") + " " + jclass.getName());
            this.setWidth(600, Sizeable.Unit.PIXELS);
            this.setHeight(450, Sizeable.Unit.PIXELS);
            this.jobj = jobj;
            this.jclass = jclass;
            this.create = create;

            // Introspect for any @FieldBuilder.* annotations
            for (Map.Entry<String, AbstractField<?>> entry : new FieldBuilder().buildBeanPropertyFields(this.jobj).entrySet()) {
                final String fieldName = entry.getKey();
                final AbstractField<?> field = entry.getValue();
                this.editorMap.put(fieldName, new Editor(field));
            }

            // Build editors for all remaining database properties
            final SortedMap<String, JField> jfieldMap = jclass.getJFieldsByName();
            for (Map.Entry<String, JField> entry : jfieldMap.entrySet()) {
                final String fieldName = entry.getKey();
                if (this.editorMap.containsKey(fieldName))
                    continue;
                final JField jfield = entry.getValue();
                final Editor editor = jfield instanceof JSimpleField ?
                  this.buildSimpleFieldEditor((JSimpleField)jfield) : new Editor(new Label("TODO: " + jfield));       // TODO
                editorMap.put(fieldName, editor);
            }

            // Create BeanItem exposing fields' properties
            this.fieldGroup.setItemDataSource(new BeanItem<JObject>(this.jobj,
              Maps.filterValues(this.editorMap, new Predicate<Editor>() {
                @Override
                public boolean apply(Editor editor) {
                    return editor.getField() != null;
                }
            }).keySet()));

            // Bind fields into FieldGroup
            for (Map.Entry<String, Editor> entry : this.editorMap.entrySet()) {
                final Editor editor = entry.getValue();
                if (editor.getField() != null)
                    this.fieldGroup.bind(editor.getField(), entry.getKey());
            }
        }

        @Override
        protected void addContent(VerticalLayout layout) {
            if (!this.create) {
                Object refLabel = MainPanel.this.objectChooser.getObjectContainer().getContainerProperty(
                  this.jobj.getObjId(), JObjectContainer.REFERENCE_LABEL_PROPERTY).getValue();
                layout.addComponent(refLabel instanceof Component ? (Component)refLabel : new Label("" + refLabel));
            }
            final FormLayout formLayout = new FormLayout();
            for (Editor editor : this.editorMap.values())
                formLayout.addComponent(editor.getComponent());
            layout.addComponent(formLayout);
        }

        @Override
        protected boolean execute() {

            // Commit fields in field group
            try {
                this.fieldGroup.commit();
            } catch (FieldGroup.CommitException e) {
                Notification.show("Invalid value(s)", null, Notification.Type.WARNING_MESSAGE);
                throw new RuntimeException(e);
            }

            // Commit transaction, if possible
            try {
                return this.writeBack();
            } catch (UnexpectedRollbackException e) {
                MainPanel.this.log.debug("ignoring UnexpectedRollbackException presumably caused by validation failure");
                return false;
            }
        }

        @RetryTransaction
        @Transactional("jsimpledbGuiTransactionManager")
        protected boolean writeBack() {
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
            this.jobj.copyTo(jtx, target.getObjId(), new ObjIdSet());

            // Run validation queue
            try {
                jtx.validate();
            } catch (ValidationException e) {
                Notification.show("Validation failed", e.getMessage(), Notification.Type.ERROR_MESSAGE);
                jtx.getTransaction().setRollbackOnly();
                return false;
            }

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

    // Editors

        protected Editor buildSimpleFieldEditor(JSimpleField jfield) {

            // Get field info
            final boolean allowNull = jfield.getGetter().getAnnotation(NotNull.class) == null
              && !jfield.getGetter().getReturnType().isPrimitive();

            // Get the property we want to edit
            final Property<?> property = this.buildProperty(jfield);

            // Build editor
            final Editor editor = this.buildSimpleFieldEditor(jfield, property, allowNull);
            editor.getComponent().setCaption(this.buildCaption(jfield.getName(), !(editor.getComponent() instanceof CheckBox)));
            return editor;
        }

        @SuppressWarnings("unchecked")
        protected Editor buildSimpleFieldEditor(JSimpleField jfield, Property<?> property, boolean allowNull) {

            // Get property type
            final Class<?> propertyType = jfield.getGetter().getReturnType();

            // Use object choosers for references
            if (jfield instanceof JReferenceField) {
                final JReferenceField refField = (JReferenceField)jfield;
                final ObjectEditor objectEditor = new ObjectEditor(this.jobj.getTransaction(), MainPanel.this.session,
                  refField.getName(), refField.getGetter().getReturnType(), (Property<JObject>)property, allowNull);
                return new Editor(objectEditor);
            }

            // Use ComboBox for Enum's
            if (Enum.class.isAssignableFrom(propertyType)) {
                final EnumComboBox comboBox = this.createEnumComboBox(propertyType.asSubclass(Enum.class), allowNull);
                comboBox.setPropertyDataSource(property);
                comboBox.setInputPrompt("Null");
                return new Editor(comboBox);
            }

            // Use DatePicker for dates
            if (Date.class.isAssignableFrom(propertyType)) {
                final PopupDateField dateField = new PopupDateField();
                dateField.setResolution(Resolution.SECOND);
                dateField.setPropertyDataSource(property);
                return new Editor(dateField, allowNull ? this.addNullCheckbox(dateField) : dateField);
            }

            // Use CheckBox for booleans
            if (propertyType == boolean.class || propertyType == Boolean.class) {
                final CheckBox checkBox = new CheckBox();
                checkBox.setPropertyDataSource(property);
                return new Editor(checkBox, allowNull ? this.addNullCheckbox(checkBox) : checkBox);
            }

            // Use text field for all other field types
            final TextField textField = new TextField();
            textField.setWidth("100%");
            textField.setNullRepresentation("");
            final FieldType<?> fieldType = this.getFieldType(jfield);
            textField.setConverter(this.buildSimpleFieldConverter(fieldType));
            textField.setPropertyDataSource(property);
            return new Editor(textField, allowNull ? this.addNullCheckbox(textField) : textField);
        }

        private HorizontalLayout addNullCheckbox(final AbstractField<?> field) {

            // Set up checkbox
            final CheckBox checkBox = new CheckBox("Null");
            checkBox.addValueChangeListener(new Property.ValueChangeListener() {
                @Override
                public void valueChange(Property.ValueChangeEvent event) {
                    final boolean setNull = checkBox.getValue();
                    field.setEnabled(!setNull);
                    if (setNull)
                        field.setValue(null);
                }
            });

            // Build layout
            final HorizontalLayout layout = new HorizontalLayout() {
                @Override
                public void attach() {
                    super.attach();
                    final Property<?> property = field.getPropertyDataSource();
                    final boolean isNull = property.getValue() == null;
                    checkBox.setValue(isNull);
                    field.setEnabled(!isNull);
                }
            };
            layout.setSpacing(true);
            layout.setMargin(false);
            layout.addComponent(field);
            layout.addComponent(checkBox);
            return layout;
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

        private String buildCaption(String fieldName, boolean includeColon) {
            return DefaultFieldFactory.createCaptionByPropertyId(fieldName) + (includeColon ? ":" : "");
        }
    }

    private static class Editor {

        private final AbstractField<?> field;
        private final Component component;

        Editor(Component component) {
            this(null, component);
        }

        Editor(AbstractField<?> field) {
            this(field, field);
        }

        Editor(AbstractField<?> field, Component component) {
            if (component == null)
                throw new IllegalArgumentException("null component");
            this.field = field;
            this.component = component;
        }

        public AbstractField<?> getField() {
            return this.field;
        }

        public Component getComponent() {
            return this.component;
        }
    }
}

