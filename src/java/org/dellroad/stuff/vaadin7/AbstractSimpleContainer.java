
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import com.vaadin.data.Container;
import com.vaadin.data.Item;
import com.vaadin.data.Property;
import com.vaadin.data.util.AbstractInMemoryContainer;
import com.vaadin.data.util.DefaultItemSorter;
import com.vaadin.data.util.filter.SimpleStringFilter;
import com.vaadin.data.util.filter.UnsupportedFilterException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Support superclass for simple read-only, in-memory {@link Container} implementations where each
 * {@link Item} in the container is backed by a Java object.
 *
 * <p>
 * The exposed properties are defined via {@link PropertyDef}s, and a {@link PropertyExtractor} is used to
 * actually extract the property values from each underlying object.
 * </p>
 *
 * <p>
 * Use {@link #load load()} to load or reload the container.
 * </p>
 *
 * @param <I> the item ID type
 * @param <T> the type of the Java objects that back each {@link Item} in the container
 *
 * @see SimpleItem
 * @see SimpleProperty
 */
@SuppressWarnings("serial")
public abstract class AbstractSimpleContainer<I, T> extends AbstractInMemoryContainer<I, String, SimpleItem<T>>
  implements Container.Filterable, Container.SimpleFilterable, Container.Sortable {

    private final HashMap<String, PropertyDef<?>> propertyMap = new HashMap<String, PropertyDef<?>>();
    private PropertyExtractor<? super T> propertyExtractor;

// Constructor

    /**
     * Constructor.
     *
     * <p>
     * After using this constructor, a subsequent invocation of {@link #setProperties setProperties()} is required
     * to define the properties of this container.
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects
     * @throws IllegalArgumentException if {@code propertyExtractor} is null
     */
    protected AbstractSimpleContainer(PropertyExtractor<? super T> propertyExtractor) {
        this.setItemSorter(new SimpleItemSorter());
        this.setPropertyExtractor(propertyExtractor);
    }

    /**
     * Primary constructor.
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects
     * @param propertyDefs container property definitions
     * @throws IllegalArgumentException if either parameter is null
     */
    protected AbstractSimpleContainer(PropertyExtractor<? super T> propertyExtractor,
      Collection<? extends PropertyDef<?>> propertyDefs) {
        this(propertyExtractor);
        this.setProperties(propertyDefs);
    }

// Public methods

    /**
     * Get the configured {@link PropertyExtractor} for this container.
     */
    public PropertyExtractor<? super T> getPropertyExtractor() {
        return this.propertyExtractor;
    }

    /**
     * Change the configured {@link PropertyExtractor} for this container.
     * Invoking this method does not result in any container notifications.
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects
     * @throws IllegalArgumentException if {@code propertyExtractor} is null
     */
    public void setPropertyExtractor(PropertyExtractor<? super T> propertyExtractor) {
        if (propertyExtractor == null)
            throw new IllegalArgumentException("null extractor");
        this.propertyExtractor = propertyExtractor;
    }

    /**
     * Change the configured properties of this container.
     *
     * @param propertyDefs container property definitions
     * @throws IllegalArgumentException if {@code propertyDefs} is null
     * @throws IllegalArgumentException if {@code propertyDefs} contains a property with a duplicate name
     */
    public void setProperties(Collection<? extends PropertyDef<?>> propertyDefs) {
        if (propertyDefs == null)
            throw new IllegalArgumentException("null propertyDefs");
        this.propertyMap.clear();
        for (PropertyDef<?> propertyDef : propertyDefs) {
            if (this.propertyMap.put(propertyDef.getName(), propertyDef) != null)
                throw new IllegalArgumentException("duplicate property name `" + propertyDef.getName() + "'");
        }
        this.fireContainerPropertySetChange();
    }

    /**
     * Change this container's contents.
     *
     * @param contents new container contents
     * @throws IllegalArgumentException if {@code contents} or any item in {@code contents} is null
     */
    public void load(Iterable<? extends T> contents) {

        // Sanity check
        if (contents == null)
            throw new IllegalArgumentException("null contents");

        // Reset item IDs
        this.resetItemIds();
        this.internalRemoveAllItems();

        // Bulk load and register items with id's 0, 1, 2, ...
        int index = 0;
        for (T obj : contents) {
            if (obj == null)
                throw new IllegalArgumentException("null item in contents at index " + index);
            SimpleItem<T> item = new SimpleItem<T>(obj, this.propertyMap, this.propertyExtractor);
            this.internalAddItemAtEnd(this.generateItemId(obj), item, false);
            index++;
        }

        // Apply filters
        this.filterAll();

        // Notify subclass
        this.afterReload();

        // Fire event
        this.fireItemSetChange();
    }

    /**
     * Get the container item ID corresponding to the given underlying Java object which is wrapped by this container.
     * Objects are tested for equality using {@link Object#equals Object.equals()}.
     *
     * <p>
     * The implementation in {@link AbstractSimpleContainer} requires a linear search of the container.
     * Some subclasses may provide a more efficient implementation.
     *
     * <p>
     * Note: items that are filtered out will not be found.
     *
     * @param obj underlying container object
     * @return item ID corresponding to {@code object}, or null if {@code object} is not found in this container
     * @throws IllegalArgumentException if {@code object} is null
     * @see #getItemIdForSame
     */
    public I getItemIdFor(T obj) {
        if (obj == null)
            throw new IllegalArgumentException("null object");
        for (I itemId : this.getItemIds()) {
            T candidate = this.getJavaObject(itemId);
            if (obj.equals(candidate))
                return itemId;
        }
        return null;
    }

    /**
     * Get the container item ID corresponding to the given underlying Java object which is wrapped by this container.
     * Objects are tested for equality using object equality, not {@link Object#equals Object.equals()}.
     *
     * <p>
     * The implementation in {@link AbstractSimpleContainer} requires a linear search of the container.
     * Some subclasses may provide a more efficient implementation.
     *
     * <p>
     * Note: items that are filtered out will not be found.
     *
     * @param obj underlying container object
     * @return item ID corresponding to {@code object}, or null if {@code object} is not found in this container
     * @throws IllegalArgumentException if {@code object} is null
     * @see #getItemIdFor
     */
    public I getItemIdForSame(T obj) {
        if (obj == null)
            throw new IllegalArgumentException("null object");
        for (I itemId : this.getItemIds()) {
            T candidate = this.getJavaObject(itemId);
            if (obj == candidate)
                return itemId;
        }
        return null;
    }

// Container and superclass required methods

    // Workaround for http://dev.vaadin.com/ticket/8856
    @Override
    @SuppressWarnings("unchecked")
    public Collection<I> getItemIds() {
        return (Collection<I>)super.getItemIds();
    }

    @Override
    public Set<String> getContainerPropertyIds() {
        return Collections.unmodifiableSet(this.propertyMap.keySet());
    }

    @Override
    public Property getContainerProperty(Object itemId, Object propertyId) {        // TODO: VAADIN7
        SimpleItem<T> entityItem = this.getItem(itemId);
        if (entityItem == null)
            return null;
        return entityItem.getItemProperty(propertyId);
    }

    @Override
    public Class<?> getType(Object propertyId) {
        PropertyDef<?> propertyDef = this.propertyMap.get(propertyId);
        return propertyDef != null ? propertyDef.getType() : null;
    }

    @Override
    public SimpleItem<T> getUnfilteredItem(Object itemId) {
        T obj = this.getJavaObject(itemId);
        if (obj == null)
            return null;
        return new SimpleItem<T>(obj, this.propertyMap, this.propertyExtractor);
    }

// Subclass methods

    /**
     * Get the underlying Java object corresponding to the given item ID.
     * This method ignores any filtering (i.e., filtered-out objects are still accessible).
     *
     * @param itemId item ID
     * @return the corresponding Java object, or null if not found
     */
    public abstract T getJavaObject(Object itemId);

    /**
     * Subclass hook invoked prior to each reload. The subclass should reset its state (e.g., issued item IDs) as required.
     */
    protected abstract void resetItemIds();

    /**
     * Create a new, unique item ID for the given object. This method is invoked during a {@linkplain #load reload operation},
     * once for each container object. Both visible and filtered objects will be passed to this method.
     *
     * <p>
     * The returned item ID must be unique, i.e., not returned by this method since the most recent invocation of
     * {@link #resetItemIds}.
     *
     * @param obj underlying container object, never null
     * @return item ID, never null
     */
    protected abstract I generateItemId(T obj);

    /**
     * Subclass hook invoked after each reload but prior to invoking {@link #fireItemSetChange}.
     *
     * <p>
     * The implementation in {@link AbstractSimpleContainer} does nothing.
     */
    protected void afterReload() {
    }

 // Container methods

    @Override
    public void sort(Object[] propertyId, boolean[] ascending) {
        super.sortContainer(propertyId, ascending);
    }

    @Override
    public void addContainerFilter(Object propertyId, String filterString, boolean ignoreCase, boolean onlyMatchPrefix) {
        try {
            this.addFilter(new SimpleStringFilter(propertyId, filterString, ignoreCase, onlyMatchPrefix));
        } catch (UnsupportedFilterException e) {
            // the filter instance created here is always valid for in-memory containers
            throw new RuntimeException("unexpected exception", e);
        }
    }

    @Override
    public void removeAllContainerFilters() {
        this.removeAllFilters();
    }

    @Override
    public void removeContainerFilters(Object propertyId) {
        this.removeFilters(propertyId);
    }

    @Override
    public void addContainerFilter(Filter filter) {
        this.addFilter(filter);
    }

    @Override
    public void removeContainerFilter(Filter filter) {
        this.removeFilter(filter);
    }

    @Override
    public Collection<?> getSortableContainerPropertyIds() {
        ArrayList<String> propertyIds = new ArrayList<String>(this.propertyMap.size());
        for (Map.Entry<String, PropertyDef<?>> entry : this.propertyMap.entrySet()) {
            if (entry.getValue().isSortable())
                propertyIds.add(entry.getKey());
        }
        return propertyIds;
    }

// ItemSorter class

    /**
     * {@link ItemSorter} implementation used by {@link SimpleContainer}.
     */
    private class SimpleItemSorter extends DefaultItemSorter {

        @Override
        @SuppressWarnings("unchecked")
        protected int compareProperty(Object propertyId, boolean ascending, Item item1, Item item2) {
            PropertyDef<?> propertyDef = AbstractSimpleContainer.this.propertyMap.get(propertyId);
            if (propertyDef == null || !propertyDef.isSortable())
                return super.compareProperty(propertyId, ascending, item1, item2);
            int diff = this.sort(propertyDef, item1, item2);
            return ascending ? diff : -diff;
        }

        // This method exists only to allow the generic parameter <V> to be bound
        private <V> int sort(PropertyDef<V> propertyDef, Item item1, Item item2) {
            return propertyDef.sort(propertyDef.read(item1), propertyDef.read(item2));
        }
    }
}

