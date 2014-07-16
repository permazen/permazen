
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;

import org.dellroad.stuff.spring.RetryTransaction;
import org.dellroad.stuff.vaadin7.PropertyDef;
import org.dellroad.stuff.vaadin7.PropertyExtractor;
import org.dellroad.stuff.vaadin7.ProvidesPropertyScanner;
import org.dellroad.stuff.vaadin7.SimpleItem;
import org.dellroad.stuff.vaadin7.SimpleKeyedContainer;
import org.dellroad.stuff.vaadin7.VaadinApplicationListener;
import org.dellroad.stuff.vaadin7.VaadinConfigurable;
import org.jsimpledb.JCollectionField;
import org.jsimpledb.JCounterField;
import org.jsimpledb.JField;
import org.jsimpledb.JListField;
import org.jsimpledb.JMapField;
import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JSimpleField;
import org.jsimpledb.JTransaction;
import org.jsimpledb.change.Change;
import org.jsimpledb.change.ChangeAdapter;
import org.jsimpledb.change.ObjectCreate;
import org.jsimpledb.change.ObjectDelete;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.UnknownFieldException;
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
    private final ProvidesPropertyScanner<?> propertyScanner;
    private final ObjPropertyDef<?> referenceLabelPropertyDef;
    private final List<String> orderedPropertyNames;

    private boolean hasReferenceLabel;
    private DataChangeListener dataChangeListener;
    private Query lastQuery;

    @Autowired
    private ReferenceLabelCache referenceLabelCache;

    @Autowired
    @Qualifier("jsimpledbGuiJSimpleDB")
    private JSimpleDB jdb;

    @Autowired
    @Qualifier("jsimpledbGuiEventMulticaster")
    private ApplicationEventMulticaster eventMulticaster;

    /**
     * Constructor.
     *
     * @param type type restriction, or null for no restriction
     */
    public <T> ObjectContainer(Class<T> type) {
        this.type = type;
        this.propertyScanner = type != null ? new ProvidesPropertyScanner<T>(type) : null;
        this.referenceLabelPropertyDef = this.buildReferenceLabelPropertyDef();
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
     * Get the type restriction associated with this instance.
     *
     * @return Java type restriction, or null if there is none
     */
    public Class<?> getType() {
        return this.type;
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
    public void reload(Query query) {

        // Get objects
        Iterable<?> objs = query.query(this.type);

        // Filter out any instances of the wrong type
        if (this.type != null) {
            objs = Iterables.filter(objs, new Predicate<Object>() {
                @Override
                public boolean apply(Object obj) {
                    return ObjectContainer.this.type.isInstance(obj);
                }
            });
        }

        // Limit the number of instances
        objs = Iterables.limit(objs, MAX_OBJECTS);

        // Copy out of transaction
        final Iterable<JObject> jobjs = Iterables.transform(objs, new Function<Object, JObject>() {
            @Override
            public JObject apply(Object obj) {
                return ((JObject)obj).copyOut();
            }
          });

        // Load them
        this.load(jobjs);
        this.lastQuery = query;
    }

    public void reload() {
        if (this.lastQuery != null)
            this.reload(this.lastQuery);
    }

// Connectable

    @Override
    public void connect() {
        super.connect();
        this.dataChangeListener = new DataChangeListener();
        this.dataChangeListener.register();
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
        pdefs.setPropertyDef(this.referenceLabelPropertyDef);

        // Add properties shared by all JObjects
        pdefs.setPropertyDef(new ObjIdPropertyDef(OBJ_ID_PROPERTY));
        pdefs.setPropertyDef(new ObjPropertyDef<SizedLabel>(TYPE_PROPERTY, SizedLabel.class) {
            @Override
            public SizedLabel extract(JObject jobj) {
                return new SizedLabel(jobj.getTransaction().getTransaction().getSchema()
                  .getVersion(jobj.getSchemaVersion()).getSchemaItem(jobj.getObjId().getStorageId(), ObjType.class).getName());
            }
        });
        pdefs.setPropertyDef(new ObjPropertyDef<SizedLabel>(VERSION_PROPERTY, SizedLabel.class) {
            @Override
            public SizedLabel extract(JObject jobj) {
                return new SizedLabel("" + jobj.getSchemaVersion());
            }
        });

        // Add properties for all fields common to all sub-types of our configured type
        final SortedMap<Integer, JField> jfields = Util.getCommonJFields(
          this.jdb.getJClasses(this.type != null ? TypeToken.of(this.type) : null));
        final HashSet<String> fieldNames = new HashSet<>();
        if (jfields != null) {
            for (JField jfield : jfields.values()) {
                fieldNames.add(jfield.getName());
                pdefs.setPropertyDef(new ObjFieldPropertyDef(jfield));
            }
        }

        // Apply any @ProvidesProperty overrides for those fields
        if (jfields != null && this.propertyScanner != null) {
            for (PropertyDef<?> propertyDef : this.propertyScanner.getPropertyDefs()) {
                if (fieldNames.contains(propertyDef.getName()))
                    pdefs.setPropertyDef(propertyDef);
            }
        }

        // Done
        return pdefs.values();
    }

// PropertyExtractor

    @Override
    @SuppressWarnings("unchecked")
    public <V> V getPropertyValue(JObject jobj, PropertyDef<V> propertyDef) {
        if (propertyDef instanceof ObjPropertyDef)
            return (V)((ObjPropertyDef<?>)propertyDef).extract(jobj);
        if (this.propertyScanner == null)
            throw new IllegalArgumentException("unknown property: " + propertyDef.getName());
        return ((PropertyExtractor<JObject>)this.propertyScanner.getPropertyExtractor()).getPropertyValue(jobj, propertyDef);
    }

// Reference label

    private ObjPropertyDef<?> buildReferenceLabelPropertyDef() {
        final Method referenceLabelMethod = this.type != null ? this.referenceLabelCache.getReferenceLabelMethod(this.type) : null;
        if (referenceLabelMethod != null) {
            this.hasReferenceLabel = true;
            return this.buildReferenceLabelPropertyDef(referenceLabelMethod);
        } else
            return new ObjIdPropertyDef(REFERENCE_LABEL_PROPERTY);
    }

    private ObjPropertyDef<Component> buildReferenceLabelPropertyDef(final Method method) {
        return new ObjPropertyDef<Component>(REFERENCE_LABEL_PROPERTY, Component.class) {
            @Override
            public Component extract(JObject jobj) {
                try {
                    try {
                        return Component.class.cast(method.invoke(jobj));
                    } catch (InvocationTargetException e) {
                        if (e.getCause() instanceof Error)
                            throw (Error)e.getCause();
                        if (e.getCause() instanceof Exception)
                            throw (Exception)e.getCause();
                        throw e;
                    }
                } catch (Error e) {
                    throw e;
                } catch (DeletedObjectException e) {
                    return new SizedLabel("<i>Missing</i>", ContentMode.HTML);
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException("unexpected error invoking method " + method, e);
                }
            }
        };
    }

// Query

    /**
     * Callback interface used when loading objects into an {@link ObjectContainer}.
     */
    public interface Query {

        /**
         * Get the objects to load into the {@link ObjectContainer}.
         * At most {@link ObjectContainer#MAX_OBJECTS} objects are shown, and objects that are
         * not instances of the type associated with the container will be filtered out (if {@code type} is not null).
         * A transaction will be open.
         *
         * @param type type restriction, or null for none
         */
        <T> Iterable<?> query(Class<T> type);
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
            try {
                if (this.jfield instanceof JSimpleField)
                    return this.handleValue(this.jfield.getValue(jtx, id));
                else if (this.jfield instanceof JCounterField)
                    return this.handleValue(((JCounterField)this.jfield).getValue(jtx, id).get());
                else if (this.jfield instanceof JCollectionField)
                    return this.handleCollectionField(((JCollectionField)this.jfield).getValue(jtx, id));
                else if (this.jfield instanceof JListField)
                    return this.handleCollectionField(((JListField)this.jfield).getValue(jtx, id));
                else if (this.jfield instanceof JMapField) {
                    final NavigableMap<?, ?> map = ((JMapField)this.jfield).getValue(jtx, id);
                    return this.handleMultiple(Iterables.transform(map.entrySet(), new Function<Map.Entry<?, ?>, Component>() {
                        @Override
                        public Component apply(Map.Entry<?, ?> entry) {
                            final HorizontalLayout layout = new HorizontalLayout();
                            layout.setMargin(false);
                            layout.setSpacing(false);
                            layout.addComponent(ObjFieldPropertyDef.this.handleValue(entry.getKey()));
                            layout.addComponent(new SizedLabel(" \u21d2 "));        // RIGHTWARDS DOUBLE ARROW
                            layout.addComponent(ObjFieldPropertyDef.this.handleValue(entry.getValue()));
                            return layout;
                        }
                    }));
                } else
                    throw new RuntimeException("internal error");
            } catch (UnknownFieldException e) {
                return new SizedLabel("<i>NA</i>", ContentMode.HTML);
            }
        }

        private Component handleCollectionField(Collection<?> col) {
            return this.handleMultiple(Iterables.transform(col, new Function<Object, Component>() {
                @Override
                public Component apply(Object item) {
                    return ObjFieldPropertyDef.this.handleValue(item);
                }
            }));
        }

        private Component handleMultiple(Iterable<Component> components) {
            final HorizontalLayout layout = new HorizontalLayout();
            layout.setMargin(false);
            layout.setSpacing(false);
            int count = 0;
            for (Component component : components) {
                if (count >= MAX_ITEMS) {
                    layout.addComponent(new SizedLabel("..."));
                    break;
                }
                if (count > 0)
                    layout.addComponent(new SizedLabel(",&#160;", ContentMode.HTML));
                layout.addComponent(component);
                count++;
            }
            return layout;
        }

        @SuppressWarnings("unchecked")
        private <T> Component handleValue(Object value) {
            if (value == null)
                return new SizedLabel("<i>Null</i>", ContentMode.HTML);
            if (value instanceof JObject)
                return ObjectContainer.this.referenceLabelPropertyDef.extract((JObject)value);
            return new SizedLabel(String.valueOf(value));
        }
    }

// PropertyDefHolder

    private static class PropertyDefHolder extends LinkedHashMap<String, PropertyDef<?>> {

        public void setPropertyDef(PropertyDef<?> propertyDef) {
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
            if (ObjectContainer.this.type != null && !ObjectContainer.this.type.isInstance(change.getObject()))
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

