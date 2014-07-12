
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.google.common.base.Converter;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.dellroad.stuff.spring.RetryTransaction;
import org.dellroad.stuff.vaadin7.PropertyDef;
import org.dellroad.stuff.vaadin7.PropertyExtractor;
import org.dellroad.stuff.vaadin7.ProvidesPropertyScanner;
import org.dellroad.stuff.vaadin7.SimpleItem;
import org.dellroad.stuff.vaadin7.SimpleKeyedContainer;
import org.dellroad.stuff.vaadin7.VaadinApplicationListener;
import org.dellroad.stuff.vaadin7.VaadinConfigurable;
import org.jsimpledb.JClass;
import org.jsimpledb.JCollectionField;
import org.jsimpledb.JField;
import org.jsimpledb.JMapField;
import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JSimpleField;
import org.jsimpledb.JTransaction;
import org.jsimpledb.change.Change;
import org.jsimpledb.change.ChangeAdapter;
import org.jsimpledb.change.ObjectCreate;
import org.jsimpledb.change.ObjectDelete;
import org.jsimpledb.core.CollectionField;
import org.jsimpledb.core.CounterField;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.FieldSwitchAdapter;
import org.jsimpledb.core.ListField;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.SetField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.core.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.transaction.annotation.Transactional;

/**
 * Container that contains all database objects of give type (up to a limit of {@link #MAX_OBJECTS}).
 */
@SuppressWarnings("serial")
@VaadinConfigurable(preConstruction = true)
public class ObjectContainer extends SimpleKeyedContainer<ObjId, JObject> {

    public static final int MAX_OBJECTS = 1000;

    public static final String REFERENCE_LABEL_PROPERTY = "$label";
    public static final String OBJ_ID_PROPERTY = "$objId";
    public static final String TYPE_PROPERTY = "$type";
    public static final String VERSION_PROPERTY = "$version";

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Class<?> type;
    private final JClass<?> jclass;             // might be null
    private final ProvidesPropertyScanner<?> propertyScanner;
    private final ObjPropertyDef<?> referenceLabelPropertyDef;
    private final List<String> orderedPropertyNames;

    private boolean hasReferenceLabel;
    private DataChangeListener dataChangeListener;

    @Autowired
    private ReferenceLabelCache referenceLabelCache;

    @Autowired
    @Qualifier("jsimpledbGuiJSimpleDB")
    private JSimpleDB jdb;

    @Autowired
    @Qualifier("jsimpledbGuiEventMulticaster")
    private ApplicationEventMulticaster eventMulticaster;

    /**
     * Construct by type. No type-specific properties will be defined.
     */
    public ObjectContainer(Class<?> type) {
        this(type, null);
    }

    /**
     * Constructor by {@link JClass}.
     */
    public ObjectContainer(JClass<?> jclass) {
        this(jclass.getTypeToken().getRawType(), jclass);
    }

    private <T> ObjectContainer(Class<T> type, JClass<?> jclass) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        this.type = type;
        this.jclass = jclass;
        this.propertyScanner = new ProvidesPropertyScanner<T>(type);
        this.referenceLabelPropertyDef = this.jclass != null ?
          this.buildReferenceLabelPropertyDef(this.jclass) : this.buildReferenceLabelPropertyDef(-1);
        final ArrayList<PropertyDef<?>> propertyDefs = new ArrayList<>(this.buildPropertyDefs());
        this.orderedPropertyNames = Collections.unmodifiableList(Lists.transform(propertyDefs,
          new Function<PropertyDef<?>, String>() {
            @Override
            public String apply(PropertyDef<?> propertyDef) {
                return propertyDef.getName();
            }
        }));
        this.setProperties(propertyDefs);
        this.setPropertyExtractor(this);
    }

    /**
     * Get the type associated with this instance.
     *
     * @return associated Java type
     */
    public Class<?> getType() {
        return this.type;
    }

    /**
     * Get the {@link JClass} associated with this instance, if any.
     *
     * @return associated {@link JClass}, possibly null
     */
    public JClass<?> getJClass() {
        return this.jclass;
    }

    /**
     * Get the properties of this container in preferred order.
     */
    public List<String> getOrderedPropertyNames() {
        return this.orderedPropertyNames;
    }

    /**
     * Does this container have a special property for the reference label? If not, reference label is just object ID.
     */
    public boolean hasReferenceLabel() {
        return this.hasReferenceLabel;
    }

    @Override
    public ObjId getKeyFor(JObject jobj) {
        return jobj.getObjId();
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    public void reload() {
        this.load(Iterables.limit(Iterables.transform(JTransaction.getCurrent().getAll(this.type), new Function<Object, JObject>() {
            @Override
            public JObject apply(Object obj) {
                return ((JObject)obj).copyOut();
            }
          }), MAX_OBJECTS));
    }

// Connectable

    @Override
    public void connect() {
        super.connect();
        this.dataChangeListener = new DataChangeListener();
        this.dataChangeListener.register();
        this.reload();
    }

    @Override
    public void disconnect() {
        if (this.dataChangeListener != null) {
            this.dataChangeListener.unregister();
            this.dataChangeListener = null;
        }
        super.disconnect();
    }

// Property derivation

    private Collection<PropertyDef<?>> buildPropertyDefs() {
        final PropertyDefHolder pdefs = new PropertyDefHolder();

        // Add reference label property
        pdefs.add(this.referenceLabelPropertyDef);

        // Add properties shared by all JObjects
        pdefs.add(new ObjIdPropertyDef(OBJ_ID_PROPERTY));
        pdefs.add(new ObjPropertyDef<SizedLabel>(TYPE_PROPERTY, SizedLabel.class) {
            @Override
            public SizedLabel extract(JObject jobj) {
                return new SizedLabel(jobj.getTransaction().getJSimpleDB().getJClass(jobj.getObjId().getStorageId()).getName());
            }
        });
        pdefs.add(new ObjPropertyDef<SizedLabel>(VERSION_PROPERTY, SizedLabel.class) {
            @Override
            public SizedLabel extract(JObject jobj) {
                return new SizedLabel("" + jobj.getSchemaVersion());
            }
        });

        // Add properties specific to object type
        if (this.jclass != null) {
            for (JField jfield : this.jclass.getJFieldsByName().values())
                pdefs.add(new ObjFieldPropertyDef(jfield));
        }

        // Add any @ProvidesProperty additions and overrides
        for (PropertyDef<?> propertyDef : this.propertyScanner.getPropertyDefs())
            pdefs.add(propertyDef);

        // Done
        return pdefs.values();
    }

// PropertyExtractor

    @Override
    @SuppressWarnings("unchecked")
    public <V> V getPropertyValue(JObject jobj, PropertyDef<V> propertyDef) {
        if (propertyDef instanceof ObjPropertyDef)
            return (V)((ObjPropertyDef<?>)propertyDef).extract(jobj);
        return ((PropertyExtractor<JObject>)this.propertyScanner.getPropertyExtractor()).getPropertyValue(jobj, propertyDef);
    }

// Reference label

    private ObjPropertyDef<?> buildReferenceLabelPropertyDef(int storageId) {
        final JClass<?> targetJClass;
        try {
            targetJClass = this.jdb.getJClass(storageId);
        } catch (IllegalArgumentException e) {              // must be due to object having old schema version
            return new ObjIdPropertyDef(REFERENCE_LABEL_PROPERTY);
        }
        return this.buildReferenceLabelPropertyDef(targetJClass);
    }

    private ObjPropertyDef<?> buildReferenceLabelPropertyDef(JClass<?> jclass) {
        final Method referenceLabelMethod = this.referenceLabelCache.getReferenceLabelMethod(jclass);
        if (referenceLabelMethod != null) {
            this.hasReferenceLabel = true;
            return this.buildReferenceLabelPropertyDef(referenceLabelMethod);
        } else
            return new ObjIdPropertyDef(REFERENCE_LABEL_PROPERTY);
    }

    private ObjPropertyDef<?> buildReferenceLabelPropertyDef(Method method) {
        return this.buildReferenceLabelPropertyDef(method.getReturnType(), method);
    }

    private <T> ObjPropertyDef<?> buildReferenceLabelPropertyDef(final Class<T> type, final Method method) {
        return Component.class.isAssignableFrom(type) ?
          this.buildComponentReferenceLabelPropertyDef(type.asSubclass(Component.class), method) :
          this.buildNonComponentReferenceLabelPropertyDef(type, method);
    }

    private <T extends Component> ObjPropertyDef<T> buildComponentReferenceLabelPropertyDef(
      final Class<T> type, final Method method) {
        return new ObjPropertyDef<T>(REFERENCE_LABEL_PROPERTY, type) {
            @Override
            public T extract(JObject jobj) {
                final Object value;
                try {
                    return type.cast(method.invoke(jobj));
                } catch (Error e) {
                    throw e;
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException("unexpected error invoking method " + method, e);
                }
            }
        };
    }

    private ObjPropertyDef<SizedLabel> buildNonComponentReferenceLabelPropertyDef(final Class<?> type, final Method method) {
        return new ObjPropertyDef<SizedLabel>(REFERENCE_LABEL_PROPERTY, SizedLabel.class) {
            @Override
            public SizedLabel extract(JObject jobj) {
                final Object value;
                try {
                    value = method.invoke(jobj);
                } catch (Error e) {
                    throw e;
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException("unexpected error invoking method " + method, e);
                }
                return new SizedLabel(String.valueOf(value));
            }
        };
    }

// ObjPropertyDef

    private abstract static class ObjPropertyDef<C extends Component> extends PropertyDef<C> {

        ObjPropertyDef(String name, Class<C> type) {
            super(name, type);
        }

        public abstract C extract(JObject jobj);
    }

// ObjIdPropertyDef

    private static class ObjIdPropertyDef extends ObjPropertyDef<SizedLabel> {

        ObjIdPropertyDef(String name) {
            super(name, SizedLabel.class);
        }

        @Override
        public SizedLabel extract(JObject jobj) {
            return new SizedLabel("<code>" + jobj.getObjId() + "</code>", ContentMode.HTML);
        }
    }

// ObjFieldPropertyDef

    private class ObjFieldPropertyDef extends ObjPropertyDef<Component> {

        private static final int MAX_ITEMS = 3;

        private final JField jfield;

        ObjFieldPropertyDef(JField jfield) {
            super(jfield.getName(), Component.class);
            this.jfield = jfield;
        }

        @Override
        public Component extract(final JObject jobj) {
            final ObjId id = jobj.getObjId();
            final JTransaction jtx = jobj.getTransaction();
            final Transaction tx = jtx.getTransaction();
            final ObjType objType = tx.getSchema().getVersion(
              tx.getSchemaVersion(id)).getSchemaItem(id.getStorageId(), ObjType.class);
            final Field<?> field = objType.getFields().get(jfield.getStorageId());
            if (field == null)
                return this.extractMissingField();          // must be due to object having old schema version
            return field.visit(new FieldSwitchAdapter<Component>() {

                @Override
                public <E> Component caseSetField(SetField<E> field) {
                    return this.handleCollectionField(field, tx.readSetField(id, field.getStorageId(), false));
                }

                @Override
                public <E> Component caseListField(ListField<E> field) {
                    return this.handleCollectionField(field, tx.readListField(id, field.getStorageId(), false));
                }

                private <T extends Collection<E>, E> Component handleCollectionField(
                  final CollectionField<T, E> field, Collection<?> col) {
                    return this.handleMultiple(Iterables.transform(col, new Function<Object, Component>() {
                        @Override
                        public Component apply(Object item) {
                            return /*FieldSwitch.this.*/handleSimpleField(
                              ((JCollectionField)jfield).getElementField(), field.getElementField(), item);
                        }
                    }));
                }

                @Override
                public <K, V> Component caseMapField(final MapField<K, V> field) {
                    final SimpleField<K> keyField = field.getKeyField();
                    final SimpleField<V> valueField = field.getValueField();
                    final NavigableMap<?, ?> map = tx.readMapField(id, field.getStorageId(), false);
                    return this.handleMultiple(Iterables.transform(map.entrySet(), new Function<Map.Entry<?, ?>, Component>() {
                        @Override
                        public Component apply(Map.Entry<?, ?> entry) {
                            final HorizontalLayout layout = new HorizontalLayout();
                            layout.setMargin(false);
                            layout.setSpacing(false);
                            layout.addComponent(/*FieldSwitch.this.*/handleSimpleField(((JMapField)jfield).getKeyField(),
                              field.getKeyField(), entry.getKey()));
                            layout.addComponent(new SizedLabel(" \u21d2 "));        // RIGHTWARDS DOUBLE ARROW
                            layout.addComponent(/*FieldSwitch.this.*/handleSimpleField(((JMapField)jfield).getValueField(),
                              field.getValueField(), entry.getValue()));
                            return layout;
                        }
                    }));
                }

                private Component handleMultiple(Iterable<Component> components) {
                    final HorizontalLayout layout = new HorizontalLayout();
                    layout.setMargin(false);
                    layout.setSpacing(false);
                    int count = 0;
                    for (Component component : components) {
                        if (count > 0)
                            layout.addComponent(new SizedLabel(", "));
                        if (count >= MAX_ITEMS) {
                            layout.addComponent(new SizedLabel("..."));
                            break;
                        }
                        layout.addComponent(component);
                        count++;
                    }
                    return layout;
                }

                @Override
                public <T> Component caseSimpleField(SimpleField<T> field) {
                    return this.handleSimpleField((JSimpleField)jfield, field, tx.readSimpleField(id, field.getStorageId(), false));
                }

                @Override
                public Component caseCounterField(CounterField field) {
                    return new SizedLabel("" + tx.readCounterField(id, field.getStorageId(), false));
                }

                @SuppressWarnings("unchecked")
                private <T> Component handleSimpleField(JSimpleField jfield, SimpleField<T> field, Object value) {
                    final Converter<?, ?> converter = jfield.getConverter(jtx);
                    if (converter != null)
                        value = ((Converter<Object, Object>)converter).convert(value);
                    if (value == null)
                        return new SizedLabel("<i>Null</i>", ContentMode.HTML);
                    if (value instanceof JObject)
                        return ObjectContainer.this.referenceLabelPropertyDef.extract((JObject)value);
                    return new SizedLabel(String.valueOf(value));
                }
            });
        }

        protected Component extractMissingField() {
            return new SizedLabel("<i>N/A</i>", ContentMode.HTML);
        }
    }

// PropertyDefHolder

    private static class PropertyDefHolder extends LinkedHashMap<String, PropertyDef<?>> {

        public void add(PropertyDef<?> propertyDef) {
            this.put(propertyDef.getName(), propertyDef);
        }
    }

// DataChangeListener

    private class DataChangeListener extends VaadinApplicationListener<DataChangeEvent> {

        DataChangeListener() {
            super(ObjectContainer.this.eventMulticaster);
            this.setAsynchronous(true);
        }

        @Override
        protected void onApplicationEventInternal(DataChangeEvent event) {

            // Determine whether the change concerns us
            final Change<?> change = event.getChange();
            if (!ObjectContainer.this.type.isInstance(change.getObject()))
                return;

            // Apply change
            change.visit(new ChangeAdapter<Void>() {

                @Override
                public <T> Void caseObjectCreate(ObjectCreate<T> change) {
                    ObjectContainer.this.reload();
                    return null;
                }

                @Override
                public <T> Void caseObjectDelete(ObjectDelete<T> change) {
                    ObjectContainer.this.reload();
                    return null;
                }

                @Override
                public <T> Void caseChange(Change<T> event) {
                    final JObject jobj = (JObject)event.getObject();
                    final SimpleItem<JObject> item = (SimpleItem<JObject>)ObjectContainer.this.getItem(jobj.getObjId());
                    if (item != null) {
                        this.copy(jobj, item.getObject().getTransaction());
                        item.fireValueChange();
                    }
                    return null;
                }

                @RetryTransaction
                @Transactional("jsimpledbGuiTransactionManager")
                private void copy(JObject jobj, JTransaction snapshot) {
                    try {
                        jobj.copyTo(snapshot, jobj.getObjId());
                    } catch (DeletedObjectException e) {
                        // ignore
                    }
                }
            });
        }
    }
}

