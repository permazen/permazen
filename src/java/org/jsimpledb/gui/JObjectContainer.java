
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

import org.dellroad.stuff.vaadin7.PropertyDef;
import org.dellroad.stuff.vaadin7.PropertyExtractor;
import org.dellroad.stuff.vaadin7.ProvidesPropertyScanner;
import org.dellroad.stuff.vaadin7.SimpleItem;
import org.dellroad.stuff.vaadin7.SimpleKeyedContainer;
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

/**
 * General purpose support superclass for Vaadin {@link com.vaadin.data.Container}s backed by {@link JSimpleDB} database objects.
 *
 * <p>
 * Instances are configured with a <b>type</b>, which can be any Java type (including interface types). The container
 * will then be restricted to database objects that are instances of the configured type. The type may be null, in which
 * case there is no restriction. The subclass {@linkplain #queryForObjects determines} which objects are actually loaded
 * into the container.
 * </p>
 *
 * <p><b>Container Properties</b></p>
 *
 * <p>
 * Instances will have the following properties:
 * <ul>
 *  <li>{@link #OBJECT_ID_PROPERTY}: Object {@link ObjId}</li>
 *  <li>{@link #TYPE_PROPERTY}: Object type name (JSimpleDB type name, not Java type name, though these are typically the same)</li>
 *  <li>{@link #VERSION_PROPERTY}: Object schema version</li>
 *  <li>{@link #REFERENCE_LABEL_PROPERTY}: Object <b>reference label</b>, which is a short {@link Component} description of the
 *      object. This property contains the return value from the type's {@link ProvidesReference
 *      &#64;ProvidesReference}-annotated method that returns a sub-type of {@link Component}, if any;
 *      otherwise it returns the same as {@link #OBJECT_ID_PROPERTY}.</i>
 *  <li>A property for every {@link JSimpleDB} field that is common to all object types that sub-type
 *      this instance's configured type. The property's ID is the field name; its value is as follows:
 *      <ul>
 *          <li>Simple fields will show their string values</li>
 *          <li>Reference fields show the {@link #REFERENCE_LABEL_PROPERTY} of the referred-to object, or "Null"
 *              if the reference is null</li>
 *          <li>Set, list, and map fields show the first few entries</li>
 *      </ul>
 *  </li>
 *  <li>A property for each {@link org.dellroad.stuff.vaadin7.ProvidesProperty &#64;ProvidesProperty}-annotated method
 *      in the specified <b>type</b>.
 *      If any such method specifies a property whose name matches a {@link JSimpleDB} field name, then that method
 *      will override the auto-generated property for that field.</li>
 * </ul>
 * </p>
 *
 * <p><b>Loading and Updating</b></p>
 *
 * <p>
 * The container is loaded by querying for {@link JObject}s within a {@link JTransaction} via the subclass-provided method
 * {@link #queryForObjects queryForObjects()}, which returns an {@link Iterable Iterable<JObject>}. This container invokes
 * {@link JObject#copyOut JObject.copyOut()} on the resulting database objects, which copies their fields into the container's
 * in-memory {@link org.jsimpledb.SnapshotJTransaction}. The latter effectively serves as the cache for the container,
 * so that database transactions may be short and are only required when (re)loading. Because the container contents are
 * in memory, with large data sets, this container should only hold one "page" of objects at a time (a "page" typically being
 * only what a human would be willing to scroll through at one time). It is up to {@link #queryForObjects queryForObjects()}
 * to determine what and how many objects are returned, their sort order, etc. However, if
 * {@link #queryForObjects queryForObjects()} returns any objects that are not instances of the configured type, they are ignored.
 * </p>
 *
 * <p>
 * Instances may be (re)loaded at any time by invoking {@link #reload}. In addition, individual objects may be updated without
 * requiring a complete reload by invoking {@link #applyChange applyChange()}. In a typical design pattern,
 * {@link org.jsimpledb.change.Change} objects originate from {@link org.jsimpledb.annotation.OnChange &#64;OnChange} methods
 * and are broadcast (after copying into memory using a {@link org.jsimpledb.change.ChangeCopier} to listeners (such as this
 * container) within a transaction synchronization callback (see {@link org.jsimpledb.core.Transaction.Callback#afterCommit}).
 * </p>
 *
 * <p>
 * The number of objects loaded is limited by a configurable maximum.
 * </p>
 *
 * <p><b>{@link ProvidesReference &#64;ProvidesReference} Limitations</b></p>
 *
 * <p>
 * The use of {@link ProvidesReference &#64;ProvidesReference} methods for reference labels/strings has certain implications.
 * First, if the method reads any of the object field(s) via their Java getter methods (as would normally be expected),
 * this will trigger a schema upgrade of the object if out of date; however, this schema upgrade will occur in the container's
 * {@link org.jsimpledb.SnapshotJTransaction} rather than in a real database transaction, so the container will show
 * a different schema version than what's in the database. The automatic schema upgrade can be avoided by reading the
 * field using the appropriate {@link JTransaction} field access method (e.g., {@link JTransaction#readSimpleField
 * JTransaction.readSimpleField()}) and being prepared to handle a {@link org.jsimpledb.core.UnknownFieldException}
 * if/when the object has an older schema version that does not contain the field.
 * </p>
 *
 * <p>
 * Secondly, because the values of reference fields (including complex sub-fields) are displayed using reference labels,
 * and these are typically derived from the referenced object's fields, those indirectly referenced fields need to be
 * copied into the container's {@link org.jsimpledb.SnapshotJTransaction} as well. The easiest way to ensure these indirectly
 * referenced objects are copied is by overriding {@link JObject#getRelatedObjects}, because this container copies
 * objects into memory by passing a null {@code refPaths} argument to {@link JObject#copyOut JObject#copyOut} (this
 * behavior is determined by {@link #copyOut}; subclasses may override as needed).
 * </p>
 */
@SuppressWarnings("serial")
public abstract class JObjectContainer extends SimpleKeyedContainer<ObjId, JObject> {

    /**
     * Default maximum number of objects.
     */
    public static final int DEFAULT_MAX_OBJECTS = 1000;

    /**
     * Container property name for the reference label property, which has type {@link Component}.
     */
    public static final String REFERENCE_LABEL_PROPERTY = "$label";

    /**
     * Container property name for the object ID property.
     */
    public static final String OBJECT_ID_PROPERTY = "$objId";

    /**
     * Container property name for the object type property.
     */
    public static final String TYPE_PROPERTY = "$type";

    /**
     * Container property name for the object schema version property.
     */
    public static final String VERSION_PROPERTY = "$version";

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * The associated {@link JSimpleDB}, provding schema information.
     */
    protected final JSimpleDB jdb;

    private final ObjIdPropertyDef objIdPropertyDef = new ObjIdPropertyDef();
    private final ObjTypePropertyDef objTypePropertyDef = new ObjTypePropertyDef();
    private final ObjVersionPropertyDef objVersionPropertyDef = new ObjVersionPropertyDef();
    private final RefLabelPropertyDef refLabelPropertyDef = new RefLabelPropertyDef();

    private Class<?> type;
    private int maxObjects = DEFAULT_MAX_OBJECTS;
    private ProvidesPropertyScanner<?> propertyScanner;
    private List<String> orderedPropertyNames;

    /**
     * Constructor.
     *
     * @param jdb {@link JSimpleDB} database
     * @param type type restriction, or null for no restriction
     * @throws IllegalArgumentException if {@code jdb} is null
     */
    protected <T> JObjectContainer(JSimpleDB jdb, Class<T> type) {
        if (jdb == null)
            throw new IllegalArgumentException("null jdb");
        this.jdb = jdb;
        this.setType(type);
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
     * Change the type restriction associated with this instance. Typically requires a reload.
     *
     * @param type Java type restriction, or null for none
     */
    public <T> void setType(Class<T> type) {
        this.type = type;
        this.propertyScanner = this.type != null ? new ProvidesPropertyScanner<T>(/*this.*/type) : null;
        final ArrayList<PropertyDef<?>> propertyDefs = new ArrayList<>(this.buildPropertyDefs());
        this.orderedPropertyNames = Collections.unmodifiableList(Lists.transform(propertyDefs,
          new Function<PropertyDef<?>, String>() {
            @Override
            public String apply(PropertyDef<?> propertyDef) {
                return propertyDef.getName();
            }
        }));
        this.setProperties(propertyDefs);
    }

    /**
     * Configure the maximum number of objects.
     */
    public void setMaxObjects(int maxObjects) {
        this.maxObjects = Math.max(maxObjects, 0);
    }

    /**
     * Get the properties of this container in preferred order.
     */
    public List<String> getOrderedPropertyNames() {
        return this.orderedPropertyNames;
    }

    @Override
    public ObjId getKeyFor(JObject jobj) {
        return jobj.getObjId();
    }

    /**
     * (Re)load this container.
     *
     * <p>
     * This method delegates to {@link #queryForObjects queryForObjects()} indirectly via {@link #doInTransaction}.
     * </p>
     */
    public void reload() {
        this.doInTransaction(new Runnable() {
            @Override
            public void run() {
                JObjectContainer.this.doReloadInTransaction();
            }
        });
    }

    /**
     * Apply the given change to this container, if applicable.
     *
     * <p>
     * The changed object {@linkplain Change#getObject referred to} by {@code change} must be accessible.
     * Unless this method is invoked from within the transaction in which the change occurred, this means that
     * {@code change} must have been be copied out of its transaction, e.g, using a {@link org.jsimpledb.change.ChangeCopier}.
     * </p>
     *
     * @throws IllegalArgumentException if {@code change} is null
     * @throws org.jsimpledb.core.StaleTransactionException if {@code change} refers to a {@link JObject}
     *  without an associated tranasction.
     */
    public <T> void applyChange(Change<T> change) {

        // Sanity check
        if (change == null)
            throw new IllegalArgumentException("null change");

        // Determine whether the change concerns us
        if (this.type != null && !this.type.isInstance(change.getObject()))
            return;

        // Apply change
        change.visit(new ChangeAdapter<Void>() {

            @Override
            public <T> Void caseObjectDelete(ObjectDelete<T> change) {
                JObjectContainer.this.reload();
                return null;
            }

            @Override
            public <T> Void caseObjectCreate(ObjectCreate<T> change) {
                JObjectContainer.this.reload();
                return null;
            }

            @Override
            public <T> Void caseChange(Change<T> event) {
                final JObject jobj = (JObject)event.getObject();
                final SimpleItem<JObject> item = (SimpleItem<JObject>)JObjectContainer.this.getItem(jobj.getObjId());
                if (item == null)
                    return null;
                try {
                    jobj.copyTo(item.getObject().getTransaction(), jobj.getObjId());
                } catch (DeletedObjectException e) {
                    // ignore
                }
                item.fireValueChange();
                return null;
            }
        });
    }

    // This method runs within a transactin
    private void doReloadInTransaction() {

        // Get objects from subclass
        Iterable<? extends JObject> jobjs = this.queryForObjects();

        // Filter out any instances of the wrong type
        if (JObjectContainer.this.type != null) {
            jobjs = Iterables.filter(jobjs, new Predicate<JObject>() {
                @Override
                public boolean apply(JObject jobj) {
                    return JObjectContainer.this.type.isInstance(jobj);
                }
            });
        }

        // Filter out any duplicate objects
        jobjs = Iterables.filter(jobjs, new Predicate<JObject>() {

            private final HashSet<ObjId> seen = new HashSet<>();

            @Override
            public boolean apply(JObject jobj) {
                return this.seen.add(jobj.getObjId());
            }
        });

        // Copy database objects out of the database transaction into my in-memory transaction as we load them
        jobjs = Iterables.transform(jobjs, new Function<JObject, JObject>() {
            @Override
            public JObject apply(JObject jobj) {
                return JObjectContainer.this.copyOut(jobj);
            }
        });

        // Limit the total number of objects in the container
        jobjs = Iterables.limit(jobjs, this.maxObjects);

        // Now actually load them
        this.load(jobjs);
    }

    /**
     * Copy the given database object out into the current {@link org.jsimpledb.SnapshotJTransaction}.
     *
     * <p>
     * The implementation in {@link JObjectContainer} returns {@link JObject#copyOut jobj.copyOut((String[])null)}.
     * Subclasses may override to specify custom reference paths to copy, etc. As an alternative,
     * consider simply implementing {@link JObject#getRelatedObjects} in Java model classes.
     * </p>
     *
     * @param jobj the object to copy
     * @return the copy of {@code jobj} in the current {@link org.jsimpledb.SnapshotJTransaction}
     */
    protected JObject copyOut(JObject jobj) {
        return jobj.copyOut((String[])null);
    }

    /**
     * Perform the given action within a {@link JTransaction}.
     *
     * @param action the action to perform
     */
    protected abstract void doInTransaction(Runnable action);

    /**
     * Query for the database objects that will be used to fill this container. Objects should be returned in the
     * desired order; any duplicates will be ignored.
     *
     * <p>
     * A {@link JTransaction} will be open in the current thread when this method is invoked.
     * </p>
     *
     * @return database objects
     */
    protected abstract Iterable<? extends JObject> queryForObjects();

// Property derivation

    private Collection<PropertyDef<?>> buildPropertyDefs() {
        final PropertyDefHolder pdefs = new PropertyDefHolder();

        // Add properties shared by all JObjects
        pdefs.setPropertyDef(this.refLabelPropertyDef);
        pdefs.setPropertyDef(this.objIdPropertyDef);
        pdefs.setPropertyDef(this.objTypePropertyDef);
        pdefs.setPropertyDef(this.objVersionPropertyDef);

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

        // Apply any @ProvidesProperty-annotated method properties, possibly overridding jfields
        if (this.propertyScanner != null) {
            for (PropertyDef<?> propertyDef : this.propertyScanner.getPropertyDefs())
                pdefs.setPropertyDef(propertyDef);
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
        final PropertyExtractor<?> propertyExtractor = this.propertyScanner.getPropertyExtractor();
        try {
            return ((PropertyExtractor<JObject>)propertyExtractor).getPropertyValue(jobj, propertyDef);
        } catch (DeletedObjectException e) {
            try {
                return propertyDef.getType().cast(new SizedLabel("<i>Missing</i>", ContentMode.HTML));
            } catch (ClassCastException e2) {
                try {
                    return propertyDef.getType().cast("(Missing)");
                } catch (ClassCastException e3) {
                    return null;
                }
            }
        }
    }

// ObjPropertyDef

    /**
     * Support superclass for {@link PropertyDef} implementations that derive the property value from a {@link JObject}.
     */
    public abstract static class ObjPropertyDef<T> extends PropertyDef<T> {

        protected ObjPropertyDef(String name, Class<T> type) {
            super(name, type);
        }

        public abstract T extract(JObject jobj);
    }

// ObjIdPropertyDef

    /**
     * Implements the {@link JObjectContainer#OBJECT_ID_PROPERTY} property.
     */
    public static class ObjIdPropertyDef extends ObjPropertyDef<SizedLabel> {

        public ObjIdPropertyDef() {
            super(OBJECT_ID_PROPERTY, SizedLabel.class);
        }

        @Override
        public SizedLabel extract(JObject jobj) {
            return new SizedLabel("<code>" + jobj.getObjId() + "</code>", ContentMode.HTML);
        }
    }

// ObjTypePropertyDef

    /**
     * Implements the {@link JObjectContainer#TYPE_PROPERTY} property.
     */
    public static class ObjTypePropertyDef extends ObjPropertyDef<SizedLabel> {

        public ObjTypePropertyDef() {
            super(TYPE_PROPERTY, SizedLabel.class);
        }

        @Override
        public SizedLabel extract(JObject jobj) {
            return new SizedLabel(jobj.getTransaction().getTransaction().getSchema()
              .getVersion(jobj.getSchemaVersion()).getSchemaItem(jobj.getObjId().getStorageId(), ObjType.class).getName());
        }
    }

// ObjVersionPropertyDef

    /**
     * Implements the {@link JObjectContainer#VERSION_PROPERTY} property.
     */
    public static class ObjVersionPropertyDef extends ObjPropertyDef<SizedLabel> {

        public ObjVersionPropertyDef() {
            super(VERSION_PROPERTY, SizedLabel.class);
        }

        @Override
        public SizedLabel extract(JObject jobj) {
            return new SizedLabel("" + jobj.getSchemaVersion());
        }
    }

// RefLabelPropertyDef

    /**
     * Implements the {@link JObjectContainer#REFERENCE_LABEL_PROPERTY} property.
     */
    public static class RefLabelPropertyDef extends ObjPropertyDef<Component> {

        public RefLabelPropertyDef() {
            super(REFERENCE_LABEL_PROPERTY, Component.class);
        }

        @Override
        public Component extract(JObject jobj) {
            final Method method = ReferenceMethodCache.getInstance().getReferenceLabelMethod(jobj.getClass());
            if (method == null)
                return new ObjIdPropertyDef().extract(jobj);
            final Object value;
            try {
                value = JObjectContainer.invoke(jobj, method);
            } catch (DeletedObjectException e) {
                return new ObjIdPropertyDef().extract(jobj);
            }
            return value instanceof Component ? (Component)value : new SizedLabel(String.valueOf(value));
        }
    }

    private static Object invoke(JObject jobj, Method method) {
        try {
            try {
                return method.invoke(jobj);
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof Error)
                    throw (Error)e.getCause();
                if (e.getCause() instanceof Exception)
                    throw (Exception)e.getCause();
                throw e;
            }
        } catch (Error e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("unexpected error invoking method " + method, e);
        }
    }

// ObjFieldPropertyDef

    /**
     * Implements a property reflecting the value of a {@link JSimpleDB} field.
     */
    public static class ObjFieldPropertyDef extends ObjPropertyDef<Component> {

        private static final int MAX_ITEMS = 3;

        private final JField jfield;

        public ObjFieldPropertyDef(JField jfield) {
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
                return new RefLabelPropertyDef().extract((JObject)value);
            return new SizedLabel(String.valueOf(value));
        }
    }

// PropertyDefHolder

    private static class PropertyDefHolder extends LinkedHashMap<String, PropertyDef<?>> {

        public void setPropertyDef(PropertyDef<?> propertyDef) {
            this.put(propertyDef.getName(), propertyDef);
        }
    }
}

