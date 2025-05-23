
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;

import io.permazen.CopyState;
import io.permazen.PermazenCollectionField;
import io.permazen.PermazenCounterField;
import io.permazen.PermazenField;
import io.permazen.PermazenFieldSwitch;
import io.permazen.PermazenMapField;
import io.permazen.PermazenObject;
import io.permazen.PermazenSimpleField;
import io.permazen.PermazenTransaction;
import io.permazen.Permazen;
import io.permazen.SnapshotJTransaction;
import io.permazen.ValidationMode;
import io.permazen.core.DeletedObjectException;
import io.permazen.encoding.Encoding;
import io.permazen.encoding.Encoding;
import io.permazen.core.ObjId;
import io.permazen.core.UnknownFieldException;
import io.permazen.core.util.ObjIdSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SortedMap;

import org.dellroad.stuff.vaadin7.PropertyDef;
import org.dellroad.stuff.vaadin7.PropertyExtractor;
import org.dellroad.stuff.vaadin7.ProvidesPropertyScanner;
import org.dellroad.stuff.vaadin7.SimpleItem;
import org.dellroad.stuff.vaadin7.SimpleKeyedContainer;
import org.dellroad.stuff.vaadin7.SortingPropertyExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vaadin {@link com.vaadin.data.Container} backed by {@link Permazen} Java model objects.
 *
 * <p>
 * Automatically creates container properties for object ID, database type, schema version, and all fields, as well as any custom
 * properties defined by {@link org.dellroad.stuff.vaadin7.ProvidesProperty &#64;ProvidesProperty}-annotated methods in
 * Java model classes. The properties of each {@link com.vaadin.data.Item} in the container are derived from a corresponding
 * {@link PermazenObject} which is usually stored in an in-memory {@link SnapshotJTransaction} (which may contain other
 * related objects, allowing an {@link com.vaadin.data.Item}'s properties to be derived from those related objects as well).
 *
 * <p>
 * Instances are configured with a <b>type</b>, which can be any Java type. The container will then be restricted to
 * database objects that are instances of the configured type. The type may be null, in which case there is no restriction.
 *
 * <p>
 * Instances are loaded by invoking {@link #load load()} with an iteration of backing {@link PermazenObject}s.
 * Normally these {@link PermazenObject}s are contained in a {@link SnapshotJTransaction}.
 *
 * <p>
 * Instances implement {@link org.dellroad.stuff.vaadin7.Connectable} and therefore must be {@link #connect connect()}'ed
 * prior to use and {@link #disconnect disconnect()}'ed after use (usually done in the associated widget's
 * {@link com.vaadin.ui.Component#attach attach()} and {@link com.vaadin.ui.Component#detach detach()} methods).
 *
 * <p>
 * <b>Container Properties</b>
 *
 * <p>
 * Instances have the following container properties:
 * <ul>
 *  <li>{@link #OBJECT_ID_PROPERTY}: Object {@link ObjId}</li>
 *  <li>{@link #TYPE_PROPERTY}: Object type name (Permazen type name, not Java type name, though the former
 *      is by default the simple Java type name)</li>
 *  <li>{@link #VERSION_PROPERTY}: Object schema version</li>
 *  <li>{@link #REFERENCE_LABEL_PROPERTY}: Object <b>reference label</b>, which is a short description identifying the
 *      object. Reference labels are used to provide "names" for objects that are more meaningful than object ID's
 *      and are used as such in other {@link Permazen} GUI classes, for example when displaying the object in a list.
 *      To customize the reference label for a Java model class,
 *      annotate a method with {@link org.dellroad.stuff.vaadin7.ProvidesProperty &#64;ProvidesProperty}{@code (}{@link
 *      JObjectContainer#REFERENCE_LABEL_PROPERTY REFERENCE_LABEL_PROPERTY}{@code )};
 *      otherwise, the value of this property will be the same as {@link #OBJECT_ID_PROPERTY}. Note that objects with
 *      customized reference labels will need to be included in the snapshot transaction also if they are referenced
 *      by an object actually in the container.
 *      </li>
 *  <li>A property for every {@link Permazen} field that is common to all object types that sub-type
 *      this containers's configured type. The property's ID is the field name; its value is as follows:
 *      <ul>
 *          <li>For simple fields, their {@linkplain Encoding#toString(Object) string form}</li>
 *          <li>For reference fields, the {@link #REFERENCE_LABEL_PROPERTY} of the referred-to object, or "Null"
 *              if the reference is null</li>
 *          <li>For set, list, and map fields, the first few entries in the collection</li>
 *      </ul>
 *  </li>
 *  <li>A property for each {@link org.dellroad.stuff.vaadin7.ProvidesProperty &#64;ProvidesProperty}-annotated method
 *      in the specified <b>type</b>. These properties will add to (or override) the properties listed above.
 * </ul>
 */
@SuppressWarnings("serial")
public class JObjectContainer extends SimpleKeyedContainer<ObjId, PermazenObject> implements SortingPropertyExtractor<PermazenObject> {

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
     * The associated {@link Permazen}.
     */
    protected final Permazen pdb;

    private final ObjIdPropertyDef objIdPropertyDef = new ObjIdPropertyDef();
    private final ObjTypePropertyDef objTypePropertyDef = new ObjTypePropertyDef();
    private final ObjVersionPropertyDef objVersionPropertyDef = new ObjVersionPropertyDef();
    private final RefLabelPropertyDef refLabelPropertyDef = new RefLabelPropertyDef();

    private Class<?> type;
    private ProvidesPropertyScanner<PermazenObject> propertyScanner;
    private List<String> orderedPropertyNames;

    /**
     * Constructor.
     *
     * @param pdb {@link Permazen} database
     * @param type type restriction, or null for no restriction
     * @throws IllegalArgumentException if {@code pdb} is null
     */
    protected JObjectContainer(Permazen pdb, Class<?> type) {
        Preconditions.checkArgument(pdb != null, "null pdb");
        this.pdb = pdb;
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
     * Change the type restriction associated with this instance.
     * Triggers a {@link com.vaadin.data.Container.PropertySetChangeEvent} and typically requires a reload.
     *
     * @param type Java type restriction, or null for none
     * @param <T> Java type
     */
    @SuppressWarnings("unchecked")
    public <T> void setType(Class<T> type) {
        this.type = type;
        this.propertyScanner = this.type != null ? (ProvidesPropertyScanner<PermazenObject>)new ProvidesPropertyScanner<T>(type) : null;
        final ArrayList<PropertyDef<?>> propertyDefs = new ArrayList<>(this.buildPropertyDefs());
        this.orderedPropertyNames = Collections.unmodifiableList(Lists.transform(propertyDefs, PropertyDef::getName));
        this.setProperties(propertyDefs);
        this.fireContainerPropertySetChange();
    }

    /**
     * Get the properties of this container in preferred order.
     *
     * @return property names
     */
    public List<String> getOrderedPropertyNames() {
        return this.orderedPropertyNames;
    }

    @Override
    public ObjId getKeyFor(PermazenObject jobj) {
        return jobj.getObjId();
    }

    /**
     * Load this container using the supplied backing {@link PermazenObject}s.
     *
     * <p>
     * A container {@link com.vaadin.data.Item} will be created wrapping each iterated {@link PermazenObject};
     * {@link com.vaadin.data.Item} properties are accessible only while the containing transaction remains open.
     *
     * @param jobjs backing {@link PermazenObject}s
     */
    @Override
    public void load(Iterable<? extends PermazenObject> jobjs) {
        this.load(jobjs.iterator());
    }

    /**
     * Load this container using the supplied backing {@link PermazenObject}s.
     *
     * <p>
     * A container {@link com.vaadin.data.Item} will be created wrapping each iterated {@link PermazenObject};
     * {@link com.vaadin.data.Item} properties are accessible only while the containing transaction remains open.
     *
     * @param jobjs backing {@link PermazenObject}s
     */
    @Override
    public void load(Iterator<? extends PermazenObject> jobjs) {

        // Filter out any instances of the wrong type
        if (this.type != null)
            jobjs = Iterators.filter(jobjs, this.type::isInstance);

        // Filter out nulls and duplicates
        final ObjIdSet seenIds = new ObjIdSet();
        jobjs = Iterators.filter(jobjs, jobj -> jobj != null && seenIds.add(jobj.getObjId()));

        // Proceed
        super.load(jobjs);
    }

    /**
     * Update a single item in this container by updating its backing object.
     *
     * <p>
     * This updates the backing object with the same object ID as {@code jobj}, if any, and then fires
     * {@link com.vaadin.data.Property.ValueChangeEvent}s for all properties of the corresponding
     * {@link com.vaadin.data.Item}.
     *
     * @param jobj updated database object
     */
    public void updateItem(PermazenObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        final SimpleItem<PermazenObject> item = (SimpleItem<PermazenObject>)this.getItem(jobj.getObjId());
        if (item != null) {
            jobj.copyTo(item.getObject().getTransaction(), new CopyState());
            item.fireValueChange();
        }
    }

    /**
     * Perform the given action within a new {@link PermazenTransaction}.
     *
     * <p>
     * The implementation in {@link JObjectContainer} performs {@code action} within a new read-only transaction.
     * Note that {@code action} should be idempotent because the transaction will be retried if needed.
     *
     * @param action the action to perform
     */
    protected void doInTransaction(Runnable action) {
        final PermazenTransaction jtx = this.pdb.createTransaction(false, ValidationMode.DISABLED);
        jtx.getTransaction().setReadOnly(true);
        try {
            jtx.performAction(action);
        } finally {
            jtx.commit();
        }
    }

// Property derivation

    private Collection<PropertyDef<?>> buildPropertyDefs() {
        final PropertyDefHolder pdefs = new PropertyDefHolder();

        // Add properties shared by all JObjects
        pdefs.setPropertyDef(this.refLabelPropertyDef);
        pdefs.setPropertyDef(this.objIdPropertyDef);
        pdefs.setPropertyDef(this.objTypePropertyDef);
        pdefs.setPropertyDef(this.objVersionPropertyDef);

        // Add properties for all fields common to all sub-types of our configured type
        final SortedMap<Integer, PermazenField> jfields = Util.getCommonJFields(this.pdb.getPermazenClasses(this.type));
        if (jfields != null) {
            for (PermazenField jfield : jfields.values())
                pdefs.setPropertyDef(new ObjFieldPropertyDef(jfield.getStorageId(), jfield.getName()));
        }

        // Apply any @ProvidesProperty-annotated method properties, possibly overridding jfields
        if (this.propertyScanner != null) {
            for (PropertyDef<?> propertyDef : this.propertyScanner.getPropertyDefs())
                pdefs.setPropertyDef(propertyDef);
        }

        // Done
        return pdefs.values();
    }

// SortingPropertyExtractor

    @Override
    @SuppressWarnings("unchecked")
    public <V> V getPropertyValue(PermazenObject jobj, PropertyDef<V> propertyDef) {
        if (propertyDef instanceof ObjPropertyDef)
            return (V)((ObjPropertyDef<?>)propertyDef).extract(jobj);
        if (this.propertyScanner == null)
            throw new IllegalArgumentException(String.format("unknown property: %s", propertyDef.getName()));
        return JObjectContainer.extractProperty(this.propertyScanner.getPropertyExtractor(), propertyDef, jobj);
    }

    @Override
    public boolean canSort(PropertyDef<?> propertyDef) {
        if (propertyDef instanceof ObjPropertyDef)
            return ((ObjPropertyDef<?>)propertyDef).canSort();
        if (this.propertyScanner == null)
            return false;
        return this.propertyScanner.getPropertyExtractor().canSort(propertyDef);
    }

    @Override
    public int sort(PropertyDef<?> propertyDef, PermazenObject jobj1, PermazenObject jobj2) {
        if (propertyDef instanceof ObjPropertyDef)
            return ((ObjPropertyDef<?>)propertyDef).sort(jobj1, jobj2);
        if (this.propertyScanner == null)
            return 0;
        return this.propertyScanner.getPropertyExtractor().sort(propertyDef, jobj1, jobj2);
    }

    @SuppressWarnings("unchecked")
    private static <V> V extractProperty(PropertyExtractor<PermazenObject> propertyExtractor, PropertyDef<V> propertyDef, PermazenObject jobj) {
        try {
            return propertyExtractor.getPropertyValue(jobj, propertyDef);
        } catch (DeletedObjectException e) {
            try {
                return propertyDef.getType().cast(new SizedLabel("<i>Unavailable</i>", ContentMode.HTML));
            } catch (ClassCastException e2) {
                try {
                    return propertyDef.getType().cast("(Unavailable)");
                } catch (ClassCastException e3) {
                    return null;
                }
            }
        }
    }

// ObjPropertyDef

    /**
     * Support superclass for {@link PropertyDef} implementations that derive the property value from a {@link PermazenObject}.
     */
    public abstract static class ObjPropertyDef<T> extends PropertyDef<T> {

        protected ObjPropertyDef(String name, Class<T> type) {
            super(name, type);
        }

        public abstract T extract(PermazenObject jobj);

        public boolean canSort() {
            return true;
        }

        public int sort(PermazenObject jobj1, PermazenObject jobj2) {
            throw new UnsupportedOperationException();
        }
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
        public SizedLabel extract(PermazenObject jobj) {
            return new SizedLabel("<code>" + jobj.getObjId() + "</code>", ContentMode.HTML);
        }

        @Override
        public int sort(PermazenObject jobj1, PermazenObject jobj2) {
            return jobj1.getObjId().compareTo(jobj2.getObjId());
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
        public SizedLabel extract(PermazenObject jobj) {
            return new SizedLabel(this.getTypeName(jobj));
        }

        @Override
        public int sort(PermazenObject jobj1, PermazenObject jobj2) {
            return this.getTypeName(jobj1).compareTo(this.getTypeName(jobj2));
        }

        private String getTypeName(PermazenObject jobj) {
            return jobj.getTransaction().getTransaction().getSchemas()
              .getVersion(jobj.getSchemaVersion()).getObjType(jobj.getObjId().getStorageId()).getName();
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
        public SizedLabel extract(PermazenObject jobj) {
            return new SizedLabel("" + jobj.getSchemaVersion());
        }

        @Override
        public int sort(PermazenObject jobj1, PermazenObject jobj2) {
            return Integer.compare(jobj1.getSchemaVersion(), jobj2.getSchemaVersion());
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
        public Component extract(PermazenObject jobj) {
            final Object value = this.getValue(jobj);
            return value instanceof Component ? (Component)value : new SizedLabel(String.valueOf(value));
        }

        @Override
        public int sort(PermazenObject jobj1, PermazenObject jobj2) {
            final Object value1 = this.getValue(jobj1);
            final Object value2 = this.getValue(jobj2);
            return value1 instanceof Component || value2 instanceof Component ?
              0 : String.valueOf(value1).compareTo(String.valueOf(value2));
        }

        private Object getValue(PermazenObject jobj) {
            final ReferenceMethodInfoCache.PropertyInfo propertyInfo
              = ReferenceMethodInfoCache.getInstance().getReferenceMethodInfo(jobj.getClass());
            if (propertyInfo == ReferenceMethodInfoCache.NOT_FOUND)
                return new ObjIdPropertyDef().extract(jobj);
            return JObjectContainer.extractProperty(propertyInfo.getPropertyExtractor(), propertyInfo.getPropertyDef(), jobj);
        }
    }

// ObjFieldPropertyDef

    /**
     * Implements a property reflecting the value of a {@link Permazen} field.
     */
    public class ObjFieldPropertyDef extends ObjPropertyDef<Component> {

        private static final int MAX_ITEMS = 3;

        private final int storageId;

        public ObjFieldPropertyDef(int storageId, String name) {
            super(name, Component.class);
            this.storageId = storageId;
        }

        @Override
        public Component extract(final PermazenObject jobj) {
            try {
                return this.getField(jobj).visit(new PermazenFieldSwitch<Component>() {

                    @Override
                    public Component casePermazenSimpleField(PermazenSimpleField field) {
                        return ObjFieldPropertyDef.this.handleValue(field.getValue(jobj));
                    }

                    @Override
                    public Component casePermazenCounterField(PermazenCounterField field) {
                        return ObjFieldPropertyDef.this.handleValue(field.getValue(jobj).get());
                    }

                    @Override
                    protected Component casePermazenCollectionField(PermazenCollectionField field) {
                        return ObjFieldPropertyDef.this.handleCollectionField(field.getValue(jobj));
                    }

                    @Override
                    public Component casePermazenMapField(PermazenMapField field) {
                        return ObjFieldPropertyDef.this.handleMultiple(Iterables.transform(
                          field.getValue(jobj).entrySet(),
                          entry -> {
                            final HorizontalLayout layout = new HorizontalLayout();
                            layout.setMargin(false);
                            layout.setSpacing(false);
                            layout.addComponent(ObjFieldPropertyDef.this.handleValue(entry.getKey()));
                            layout.addComponent(new SizedLabel(" \u21d2 "));        // RIGHTWARDS DOUBLE ARROW
                            layout.addComponent(ObjFieldPropertyDef.this.handleValue(entry.getValue()));
                            return layout;
                        }));
                    }
                });
            } catch (UnknownFieldException e) {
                return new SizedLabel("<i>NA</i>", ContentMode.HTML);
            }
        }

        @Override
        public boolean canSort() {
            return true;
        }

        @Override
        public int sort(PermazenObject jobj1, PermazenObject jobj2) {
            try {

                // Get fields
                final PermazenField jfield1 = this.getField(jobj1);
                final PermazenField jfield2 = this.getField(jobj2);

                // Compare using core API encoding
                return jfield1.visit(new PermazenFieldSwitch<Integer>() {

                    @Override
                    public Integer casePermazenSimpleField(PermazenSimpleField field1) {
                        if (!(jfield2 instanceof PermazenSimpleField))
                            return 0;
                        final PermazenSimpleField field2 = (PermazenSimpleField)jfield2;
                        final Encoding<?> encoding1 = field1.getEncoding();
                        final Encoding<?> encoding2 = field2.getEncoding();
                        if (!encoding1.equals(encoding2))
                            return 0;
                        Object value1 = field1.getValue(jobj1);
                        Object value2 = field2.getValue(jobj2);
                        final Converter<?, ?> converter1 = field1.getConverter(jobj1.getTransaction());
                        if (converter1 != null)
                            value1 = this.convert(converter1.reverse(), value1);
                        final Converter<?, ?> converter2 = field2.getConverter(jobj2.getTransaction());
                        if (converter2 != null)
                            value2 = this.convert(converter2.reverse(), value2);
                        return this.compare(encoding1, value1, value2);
                    }

                    @SuppressWarnings("unchecked")
                    private <R, S> S convert(Converter<R, S> converter, Object value) {
                        return converter.convert((R)value);
                    }

                    private <S> int compare(Encoding<S> encoding, Object v1, Object v2) {
                        return encoding.compare(encoding.validate(v1), encoding.validate(v2));
                    }

                    @Override
                    public Integer casePermazenCounterField(PermazenCounterField field1) {
                        if (!(jfield2 instanceof PermazenCounterField))
                            return 0;
                        final PermazenCounterField field2 = (PermazenCounterField)jfield2;
                        return Long.compare(field1.getValue(jobj1).get(), field2.getValue(jobj2).get());
                    }

                    @Override
                    protected Integer casePermazenField(PermazenField field) {
                        return 0;
                    }
                });
            } catch (UnknownFieldException e) {
                return 0;
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (!super.equals(obj))
                return false;
            final ObjFieldPropertyDef that = (ObjFieldPropertyDef)obj;
            return this.storageId == that.storageId;
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ this.storageId;
        }

        private PermazenField getField(final PermazenObject jobj) {
            return JObjectContainer.this.pdb.getPermazenClass(jobj.getObjId()).getField(this.storageId, PermazenField.class);
        }

        private Component handleCollectionField(Collection<?> col) {
            return this.handleMultiple(Iterables.transform(col, this::handleValue));
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
            if (value instanceof PermazenObject)
                return new RefLabelPropertyDef().extract((PermazenObject)value);
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
