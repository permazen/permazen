
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
 * Simple read-only, in-memory {@link Container} implementation where each {@link Item} is backed by a Java object.
 *
 * <p>
 * The exposed properties are defined via {@link PropertyDef}s, and a {@link PropertyExtractor} is used to
 * actually extract the property values from each underlying object.
 *
 * @param <T> the type of the Java objects that back each {@link Item} in the container
 * @see SimpleItem
 * @see SimpleProperty
 */
@SuppressWarnings("serial")
public class SimpleContainer<T> extends AbstractInMemoryContainer<Integer, String, SimpleItem<T>>
  implements Container.Filterable, Container.SimpleFilterable, Container.Sortable {

    private final HashMap<String, PropertyDef<?>> propertyMap = new HashMap<String, PropertyDef<?>>();
    private final PropertyExtractor<? super T> propertyExtractor;

    private ArrayList<SimpleItem<T>> items = new ArrayList<SimpleItem<T>>(0);

// Constructor

    /**
     * Constructor.
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects
     * @param propertyDefs container property definitions
     * @throws IllegalArgumentException if {@code propertyExtractor} is null
     */
    public SimpleContainer(PropertyExtractor<? super T> propertyExtractor, Collection<? extends PropertyDef<?>> propertyDefs) {
        if (propertyExtractor == null)
            throw new IllegalArgumentException("null extractor");
        this.propertyExtractor = propertyExtractor;
        this.setProperties(propertyDefs);
        this.setItemSorter(new SimpleItemSorter());
    }

// Public methods

    /**
     * Change the configured properties of this container.
     *
     * @param propertyDefs container property definitions
     * @throws IllegalArgumentException if {@code propertyDefs} is null
     */
    public void setProperties(Collection<? extends PropertyDef<?>> propertyDefs) {
        if (propertyDefs == null)
            throw new IllegalArgumentException("null propertyDefs");
        this.propertyMap.clear();
        for (PropertyDef<?> propertyDef : propertyDefs)
            this.propertyMap.put(propertyDef.getName(), propertyDef);
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

        // Bulk load and register items with id's 0, 1, 2, ...
        this.items = new ArrayList<SimpleItem<T>>();
        int index = 0;
        for (T obj : contents) {
            if (obj == null)
                throw new IllegalArgumentException("null item in contents at index " + this.items.size());
            SimpleItem<T> item = new SimpleItem<T>(obj, this.propertyMap, this.propertyExtractor);
            this.internalAddItemAtEnd(index++, item, false);
            this.items.add(item);
        }

        // Apply filters
        this.filterAll();

        // Notify subclass
        this.afterReload();

        // Fire event
        this.fireItemSetChange();
    }

    /**
     * Return the number of items in this container. This includes items that have been filtered out.
     */
    public int size() {
        return this.getAllItemIds().size();
    }

    /**
     * Get the underlying Java object corresponding to the given item ID.
     * This method ignores any filtering (i.e., filtered-out objects are still accessible).
     *
     * @param itemId item ID
     * @return the corresponding Java object, or null if not found
     */
    public T getJavaObject(int index) {
        if (index < 0 || index >= this.items.size())
            return null;
        return this.items.get(index).getObject();
    }

    /**
     * Get the container item ID corresponding to the given underlying Java object which is wrapped by this container.
     * Objects are tested for equality using object equality, not {@link Object#equals Object.equals()}.
     *
     * @return item ID corresponding to {@code object}, or null if {@code object} is not found in this container
     * @throws IllegalArgumentException if {@code object} is null
     */
    public Integer getItemIdFor(Object object) {
        if (object == null)
            throw new IllegalArgumentException("null object");
        for (int i = 0; i < this.items.size(); i++) {
            if (this.items.get(i).getObject() == object)
                return i;
        }
        return null;
    }

// Container and superclass required methods

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
        if (!(itemId instanceof Integer))
            return null;
        int index = (Integer)itemId;
        if (index < 0 || index >= this.items.size())
            return null;
        return this.items.get(index);
    }

// Subclass methods

    /**
     * Subclass hook invoked after each reload but prior to invoking {@link #fireItemSetChange}.
     *
     * <p>
     * The implementation in {@link SimpleContainer} does nothing.
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
            PropertyDef<?> propertyDef = SimpleContainer.this.propertyMap.get(propertyId);
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

