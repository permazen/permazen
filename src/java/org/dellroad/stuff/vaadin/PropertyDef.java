
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import com.vaadin.data.Container;
import com.vaadin.data.Item;
import com.vaadin.data.Property;
import com.vaadin.data.util.ObjectProperty;
import com.vaadin.ui.Table;

import java.util.Comparator;

/**
 * Defines a Vaadin property, having a name, which is also the property ID, and its type.
 * This class provides a mechanism for the explicit naming and identification of Vaadin properties.
 *
 * <p>
 * For a given instance, <code>def.{@link #getPropertyId getPropertyId()}</code> is the property ID
 * and you can access the value using <code>def.{@link #read read(item)}</code>.
 *
 * <p>
 * Example:
 * <blockquote><pre>
 *  PropertyDef&lt;Integer&gt; def = new PropertyDef&lt;Integer&gt;("age", Integer.class, -1);
 *  def.addTo(container);
 *  def.addTo(item, property);
 *  ...
 *  int age = def.read(item);
 *  ...
 *  Property prop = this.get(container, itemId);
 * </pre></blockquote>
 *
 * @param <T> property's value type
 */
public class PropertyDef<T> {

    /**
     * Comparator that sorts instances by name.
     */
    public static final Comparator<PropertyDef<?>> SORT_BY_NAME = new Comparator<PropertyDef<?>>() {
        @Override
        public int compare(PropertyDef<?> p1, PropertyDef<?> p2) {
            return p1.getName().compareTo(p2.getName());
        }
    };

    private final String name;
    private final Class<T> type;
    private final T defaultValue;

    /**
     * Convenience contructor.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><pre>
     *  PropertyDef(name, type, null);
     *  </pre></blockquote>
     * </p>
     */
    public PropertyDef(String name, Class<T> type) {
        this(name, type, null);
    }

    /**
     * Primary constructor.
     *
     * @param name property name; also serves as the property ID
     * @param type property type
     * @param defaultValue default value for the property; may be null
     */
    public PropertyDef(String name, Class<T> type, T defaultValue) {
        if (name == null)
            throw new IllegalArgumentException("null name");
        if (type == null)
            throw new IllegalArgumentException("null type");
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
    }

    /**
     * Get the name of this property. This is also used as the property ID.
     *
     * @return property name, never null
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the ID of this property. Returns the same thing as {@link #getName()}.
     *
     * @return property name, never null
     */
    public String getPropertyId() {
        return this.getName();
    }

    /**
     * Get the type of the property value that this instance represents.
     *
     * @return property type, never null
     */
    public Class<T> getType() {
        return this.type;
    }

    /**
     * Get the default value for this property.
     *
     * @return property default value
     */
    public T getDefaultValue() {
        return this.defaultValue;
    }

    /**
     * Create a simple {@link ObjectProperty} using the given value.
     *
     * @param value property value
     * @param readOnly whether property should be read-only
     * @return new property
     */
    public ObjectProperty<T> createProperty(T value, boolean readOnly) {
        return new ObjectProperty<T>(value, this.getType(), readOnly);
    }

    /**
     * Create a read/write {@link ObjectProperty} using the given value.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><pre>
     *  {@link #createProperty(Object, boolean) createProperty()}(value, false);
     *  </pre></blockquote>
     * </p>
     *
     * @param value property value
     * @return new property
     */
    public ObjectProperty<T> createProperty(T value) {
        return this.createProperty(value, false);
    }

    /**
     * Create a read/write {@link ObjectProperty} using the default value.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><pre>
     *  {@link #createProperty(Object) createProperty()}(def.getDefaultValue());
     *  </pre></blockquote>
     * </p>
     *
     * @return new property
     */
    public ObjectProperty<T> createProperty() {
        return this.createProperty(this.getDefaultValue());
    }

    /**
     * Get the property that this instance represents from the given {@link Item}.
     *
     * @return property, or null if not found
     * @throws ClassCastException if the property found has a different type than this instance
     */
    public Property get(Item item) {
        return this.cast(item.getItemProperty(this.getPropertyId()));
    }

    /**
     * Get the property that this instance represents from the given {@link Container}.
     *
     * @param container the container containing the items
     * @param itemId the ID of the item containing the property
     * @return property, or null if not found
     * @throws ClassCastException if the property found has a different type than this instance
     */
    public Property get(Container container, Object itemId) {
        return this.cast(container.getContainerProperty(itemId, this.getPropertyId()));
    }

    /**
     * Verify that the given property has the same Java type as this property definition.
     *
     * <p>
     * This essentially verifies that <code>property.getType() == this.getType()</code>.
     *
     * @param property the property to verify; may be null
     * @return null if {@code property} is null, otherwise {@code property}
     * @throws ClassCastException if {@code property} has a different type than this definition
     */
    public Property cast(Property property) {
        if (property == null)
            return null;
        if (property.getType() != this.getType()) {
            throw new ClassCastException("property type " + property.getType().getName()
              + " != definition type " + this.getType().getName());
        }
        return property;
    }

    /**
     * Add a property represented by this instance to the given {@link Container}.
     *
     * @param container the container to add the property to
     * @return true if the operation succeeded, false if not
     * @throws UnsupportedOperationException if the operation is not supported
     */
    public boolean addTo(Container container) {
        return container.addContainerProperty(this.getPropertyId(), this.getType(), this.getDefaultValue());
    }

    /**
     * Add a property represented by this instance to the given {@link Item}.
     *
     * @param item the item to add the property to
     * @param property the property to be added to the item and identified by this instance's name
     * @return true if the operation succeeded, false if not
     * @throws UnsupportedOperationException if the operation is not supported
     */
    public boolean addTo(Item item, Property property) {
        return item.addItemProperty(this.getPropertyId(), property);
    }

    /**
     * Add a property represented by this instance to the given {@link Table}.
     *
     * @param table the table to add the property to
     * @return true if the operation succeeded, false if not
     * @throws UnsupportedOperationException if the operation is not supported
     */
    public boolean addTo(Table table) {
        return table.addContainerProperty(this.getPropertyId(), this.getType(), this.getDefaultValue());
    }

    /**
     * Read the property that this instance represents from the given {@link Item}.
     *
     * @return property value, or null if not found
     * @throws ClassCastException if the property in {@code item} has the wrong type
     */
    public T read(Item item) {
        Property property = this.get(item);
        if (property == null)
            return null;
        return this.type.cast(property.getValue());
    }

    /**
     * Determine whether this instance supports sorting property values.
     *
     * <p>
     * The implementation in {@link PropertyDef} returns true if this instance's type implements {@link Comparable}.
     *
     * @see #sort sort()
     */
    public boolean isSortable() {
        return Comparable.class.isAssignableFrom(this.type);
    }

    /**
     * Sort two values of this property. Optional operation.
     *
     * <p>
     * The implementation in {@link PropertyDef} sorts null values first, then delegates to {@link Comparable}
     * if the values implement it, or else throws {@link UnsupportedOperationException}.
     *
     * @param value1 first value, possibly null
     * @param value2 second value, possibly null
     * @return negative, zero, or positive based on comparing {@code value1} to {@code value2}
     * @throws UnsupportedOperationException if this instance does not support sorting property values
     */
    @SuppressWarnings("unchecked")
    public int sort(T value1, T value2) {
        if ((value1 == null) != (value2 == null))
            return value1 == null ? -1 : 1;
        if (value1 == null && value2 == null)
            return 0;
        try {
            return ((Comparable<T>)value1).compareTo(value2);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    @Override
    public int hashCode() {
        return this.name.hashCode()
          ^ this.type.hashCode()
          ^ (this.defaultValue != null ? this.defaultValue.hashCode() : 0);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final PropertyDef<?> that = (PropertyDef<?>)obj;
        return this.name.equals(that.name)
          && this.type == that.type
          && (this.defaultValue != null ? this.defaultValue.equals(that.defaultValue) : that.defaultValue == null);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[name=\"" + this.name + "\",type="
          + this.type.getName() + ",defaultValue=" + this.defaultValue + "]";
    }
}

