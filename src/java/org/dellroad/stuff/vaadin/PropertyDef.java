
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
public final class PropertyDef<T> {

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
    public Object getPropertyId() {
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
    public ObjectProperty createProperty(T value, boolean readOnly) {
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
    public ObjectProperty createProperty(T value) {
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
    public ObjectProperty createProperty() {
        return this.createProperty(this.getDefaultValue());
    }

    /**
     * Get the property that this instance represents from the given {@link Item}.
     *
     * @return property, or null if not found
     */
    public Property get(Item item) {
        return item.getItemProperty(this.getPropertyId());
    }

    /**
     * Get the property that this instance represents from the given {@link Container}.
     *
     * @param container the container containing the items
     * @param itemId the ID of the item containing the property
     * @return property, or null if not found
     */
    public Property get(Container container, Object itemId) {
        return container.getContainerProperty(itemId, this.getPropertyId());
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

    @Override
    public int hashCode() {
        return this.name.hashCode()
          ^ this.type.hashCode()
          ^ (this.defaultValue != null ? this.defaultValue.hashCode() : 0);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PropertyDef))
            return false;
        PropertyDef<?> that = (PropertyDef<?>)obj;
        return this.name.equals(that.name)
          && this.type == that.type
          && (this.defaultValue != null ? this.defaultValue.equals(that.defaultValue) : that.defaultValue == null);
    }
}

