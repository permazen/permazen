
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.google.common.reflect.TypeToken;
import com.vaadin.data.Property;
import com.vaadin.data.fieldgroup.FieldGroup;
import com.vaadin.data.util.AbstractProperty;
import com.vaadin.data.util.MethodProperty;
import com.vaadin.data.util.ObjectProperty;
import com.vaadin.data.util.PropertysetItem;
import com.vaadin.server.Sizeable;
import com.vaadin.server.VaadinSession;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.CustomField;
import com.vaadin.ui.DefaultFieldFactory;
import com.vaadin.ui.Field;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.TextField;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;

import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.validation.constraints.NotNull;

import org.dellroad.stuff.spring.RetryTransaction;
import org.dellroad.stuff.vaadin7.EnumComboBox;
import org.dellroad.stuff.vaadin7.FieldBuilder;
import org.dellroad.stuff.vaadin7.VaadinConfigurable;
import org.dellroad.stuff.vaadin7.VaadinUtil;
import org.jsimpledb.CopyState;
import org.jsimpledb.Counter;
import org.jsimpledb.JClass;
import org.jsimpledb.JCounterField;
import org.jsimpledb.JField;
import org.jsimpledb.JFieldSwitchAdapter;
import org.jsimpledb.JListField;
import org.jsimpledb.JMapField;
import org.jsimpledb.JObject;
import org.jsimpledb.JSetField;
import org.jsimpledb.JSimpleField;
import org.jsimpledb.JTransaction;
import org.jsimpledb.ValidationException;
import org.jsimpledb.change.ObjectCreate;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.parse.ParseSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.annotation.Transactional;

/**
 * GUI window for editing a database object.
 */
@SuppressWarnings("serial")
@VaadinConfigurable
public class JObjectEditorWindow extends ConfirmWindow {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final JObject jobj;
    private final JClass<?> jclass;
    private final boolean create;
    private final Component titleComponent;
    private final ParseSession session;
    private final FieldGroup fieldGroup = new FieldGroup();
    private final TreeMap<String, Field<?>> fieldMap = new TreeMap<>();

    @Autowired(required = false)
    private ChangePublisher changePublisher;

    /**
     * Constructor for creating and editing a new object.
     *
     * @param ui associated {@link UI}
     * @param session parse session for {@link JObjectChooser}
     * @param jclass database object type
     */
    public JObjectEditorWindow(UI ui, ParseSession session, JClass<?> jclass) {
        this(ui, session, jclass, null, null);
    }

    /**
     * Constructor for editing an existing object.
     *
     * @param ui associated {@link UI}
     * @param session parse session for {@link JObjectChooser}
     * @param jobj object to edit; should be contained in a snapshot transaction
     * @param titleComponent title for edit panel
     */
    @SuppressWarnings("unchecked")
    public JObjectEditorWindow(UI ui, ParseSession session, JClass<?> jclass, JObject jobj, Component titleComponent) {
        super(ui, (jobj != null ? "Edit " : "New ") + jclass.getName());
        this.setWidth(600, Sizeable.Unit.PIXELS);
        this.setHeight(450, Sizeable.Unit.PIXELS);
        this.jclass = jclass;
        this.jobj = jobj != null ? jobj : this.doCreateForEdit();
        this.create = jobj == null;
        this.session = session;
        this.titleComponent = titleComponent;

        // Create PropertysetItem to hold our editable properties
        final PropertysetItem item = new PropertysetItem();

        // Introspect for any @FieldBuilder.* annotations
        this.fieldMap.putAll(new FieldBuilder().buildBeanPropertyFields(this.jobj));

        // Build fields and components for all remaining database properties
        final SortedMap<String, JField> jfieldMap = jclass.getJFieldsByName();
        for (Map.Entry<String, JField> entry : jfieldMap.entrySet()) {
            final String fieldName = entry.getKey();
            final JField jfield = entry.getValue();

            // If a Field already exists for this database field, just use it, otherwise build one
            Field<?> field = this.fieldMap.get(fieldName);
            if (field == null) {
                field = this.buildFieldField(fieldName, jfield);
                this.fieldMap.put(fieldName, field);
            }

            // Build an appropriate Vaadin Property for the field
            item.addItemProperty(fieldName, this.buildFieldProperty(this.jobj, jfield));

            // Set the field's caption
            field.setCaption(this.buildCaption(jfield.getName(), !(field instanceof CheckBox)));
        }

        // Connect fields to properties via FieldGroup
        this.fieldGroup.setItemDataSource(item);

        // Bind fields into FieldGroup
        for (Map.Entry<String, Field<?>> entry : this.fieldMap.entrySet())
            this.fieldGroup.bind(entry.getValue(), entry.getKey());
    }

    @Override
    protected void addContent(VerticalLayout layout) {
        if (this.titleComponent != null)
            layout.addComponent(this.titleComponent);
        final FormLayout formLayout = new FormLayout();
        for (Field<?> field : this.fieldMap.values())
            formLayout.addComponent(field);
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
            this.log.debug("ignoring UnexpectedRollbackException presumably caused by validation failure");
            return false;
        }
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    private JObject doCreateForEdit() {
        return (JObject)JTransaction.getCurrent().getSnapshotTransaction().create(this.jclass);
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    protected boolean writeBack() {

        // Find/create target object in current transaction
        final JTransaction jtx = JTransaction.getCurrent();
        final ObjId id = this.jobj.getObjId();
        final JObject target = this.create ? (JObject)jtx.create(this.jclass) : jtx.getJObject(id);

        // Verify object still exists when editing
        if (!this.create && !target.exists()) {
            Notification.show("Object " + id + " no longer exists", null, Notification.Type.WARNING_MESSAGE);
            return true;
        }

        // Copy fields
        this.jobj.copyTo(jtx, target.getObjId(), new CopyState());

        // Run validation queue
        try {
            jtx.validate();
        } catch (ValidationException e) {
            Notification.show("Validation failed", e.getMessage(), Notification.Type.ERROR_MESSAGE);
            jtx.getTransaction().setRollbackOnly();
            return false;
        }

        // Broadcast update event after successful commit
        if (this.changePublisher != null) {
            if (create)
                this.changePublisher.publishChangeOnCommit(new ObjectCreate<Object>(target));
            else
                this.changePublisher.publishChangeOnCommit(target);
        }

        // Show notification after successful commit
        final VaadinSession vaadinSession = VaadinUtil.getCurrentSession();
        jtx.getTransaction().addCallback(new Transaction.CallbackAdapter() {
            @Override
            public void afterCommit() {
                VaadinUtil.invoke(vaadinSession, new Runnable() {
                    @Override
                    public void run() {
                        Notification.show((JObjectEditorWindow.this.create ? "Created" : "Updated") + " object " + id);
                    }
                });
            }
        });
        return true;
    }

// Field Builders

    private Field<?> buildFieldField(String fieldName, JField jfield) {
        return jfield.visit(new JFieldSwitchAdapter<Field<?>>() {
            @Override
            public Field<?> caseJSimpleField(JSimpleField jfield) {
                final boolean allowNull = jfield.getGetter().getAnnotation(NotNull.class) == null
                  && !jfield.getTypeToken().isPrimitive();
                return new SimpleFieldFieldBuilder(JObjectEditorWindow.this.jobj.getTransaction(),
                  jfield, JObjectEditorWindow.this.session, allowNull).buildField();
            }
            @Override
            public Field<?> caseJCounterField(JCounterField jfield) {
                final TextField field = new TextField();
                field.setWidth("100%");
                field.setNullSettingAllowed(false);
                field.setConverter(new SimpleFieldConverter<Long>(JObjectEditorWindow.this.jclass.getJSimpleDB()
                  .getDatabase().getFieldTypeRegistry().getFieldType(TypeToken.of(long.class))));
                return field;
            }
            @Override
            public Field<?> caseJSetField(JSetField jfield) {
                //return new SetFieldFieldBuilder(jfield, JObjectEditorWindow.this.session).buildField();
                return new PlaceHolderField(jfield);                                       // TODO
            }
            @Override
            public Field<?> caseJListField(JListField jfield) {
                //return new ListFieldFieldBuilder(jfield, JObjectEditorWindow.this.session).buildField();
                return new PlaceHolderField(jfield);                                       // TODO
            }
            @Override
            public Field<?> caseJMapField(JMapField jfield) {
                //return new MapFieldFieldBuilder(jfield, JObjectEditorWindow.this.session).buildField();
                return new PlaceHolderField(jfield);                                       // TODO
            }
        });
    }

    private Property<?> buildFieldProperty(final JObject jobj, JField jfield) {
        return jfield.visit(new JFieldSwitchAdapter<Property<?>>() {
            @Override
            @SuppressWarnings({ "unchecked", "rawtypes" })
            public Property<?> caseJSimpleField(JSimpleField jfield) {
                return new MethodProperty(jfield.getTypeToken().getRawType(), jobj, jfield.getGetter(), jfield.getSetter());
            }
            @Override
            public Property<?> caseJCounterField(JCounterField jfield) {
                return new CounterProperty(jfield.getValue(jobj));
            }
            @Override
            @SuppressWarnings("rawtypes")
            public Property<?> caseJSetField(JSetField jfield) {
                return new CollectionProperty(jfield.getValue(jobj));
            }
            @Override
            @SuppressWarnings("rawtypes")
            public Property<?> caseJListField(JListField jfield) {
                return new CollectionProperty(jfield.getValue(jobj));
            }
            @Override
            public Property<?> caseJMapField(JMapField jfield) {
                return new ObjectProperty<Void>(null, Void.class);                          // TODO
            }
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <T extends Enum> EnumComboBox createEnumComboBox(Class<T> enumType, boolean allowNull) {
        return new EnumComboBox(enumType, allowNull);
    }

    // This method exists solely to bind the generic type parameters
    private <T> SimpleFieldConverter<T> buildSimpleFieldConverter(FieldType<T> fieldType) {
        return new SimpleFieldConverter<T>(fieldType);
    }

    private String buildCaption(String fieldName, boolean includeColon) {
        return DefaultFieldFactory.createCaptionByPropertyId(fieldName) + (includeColon ? ":" : "");
    }

    // This method exists solely to bind the generic type parameters
    private <T> NullableField<T> addNullButton(Field<T> field) {
        return new NullableField<T>(field);
    }

// CounterProperty

    private static class CounterProperty extends AbstractProperty<Long> {

        private final Counter counter;

        CounterProperty(Counter counter) {
            this.counter = counter;
        }

        @Override
        public Class<Long> getType() {
            return Long.class;
        }

        @Override
        public Long getValue() {
            return this.counter.get();
        }

        @Override
        public void setValue(Long value) {
            this.counter.set(value != null ? value : 0);
        }
    }

// CollectionProperty

    @SuppressWarnings("rawtypes")
    private static class CollectionProperty extends AbstractProperty<Collection> {

        private final Collection collection;

        CollectionProperty(Collection collection) {
            this.collection = collection;
        }

        @Override
        public Class<Collection> getType() {
            return Collection.class;
        }

        @Override
        public Collection getValue() {
            return this.collection;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void setValue(Collection value) {
            throw new UnsupportedOperationException();
            //this.collection.clear();
            //this.collection.addAll(value);
        }
    }

// PlaceHolderField

    @SuppressWarnings("serial")
    private static class PlaceHolderField extends CustomField<Object> {

        private final JField jfield;

        PlaceHolderField(JField jfield) {
            this.jfield = jfield;
        }

    // CustomField

        @Override
        public Class<Object> getType() {
            return Object.class;
        }

        @Override
        protected Label initContent() {
            return new Label("TODO: editor for field `" + this.jfield.getName() + "'");
        }
    }
}

