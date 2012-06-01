
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import com.vaadin.data.Container;
import com.vaadin.data.Item;
import com.vaadin.data.Property;
import com.vaadin.data.util.AbstractContainer;

import java.util.AbstractList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Support superclass for read-only {@link Container} implementations where each {@link Item} in the container
 * is backed by a Java object, and the Java objects are accessed via a query returning an ordered "query list".
 * The container's {@link Item} ID's are simply the index of the corresponding objects in this list.
 *
 * <p>
 * This class invokes {@link #query} to generate the query list. The query list is then cached, but this class will invoke
 * {@link #validate validate()} prior to each subsequent use to ensure it is still usable.
 * If not, {@link #query} is invoked to regenerate it.
 *
 * <p>
 * Note that the query list being invalid is an orthogonal concept from the contents of the list having changed;
 * however, the latter implies the former (but not vice-versa). Therefore, after any change to the list content,
 * first {@link #invalidate} and then {@link #fireItemSetChange} should be invoked. On the other hand, the list can
 * become invalid without the content changing if e.g., the list contains JPA entities and the corresponding
 * {@link javax.persistence.EntityManager} is closed.
 *
 * <p>
 * The subclass may forcibly invalidate the current query list via {@link #invalidate}, e.g., after change to the
 * list content.
 *
 * @param <T> the type of the Java objects that back each {@link Item} in the container
 */
@SuppressWarnings("serial")
public abstract class AbstractQueryContainer<T> extends AbstractContainer implements Container.Ordered, Container.Indexed,
  Container.PropertySetChangeNotifier, Container.ItemSetChangeNotifier {

    private final HashMap<String, PropertyDef<?>> propertyMap = new HashMap<String, PropertyDef<?>>();
    private PropertyExtractor<? super T> propertyExtractor;

    private List<T> currentList;

// Constructors

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
    protected AbstractQueryContainer(PropertyExtractor<? super T> propertyExtractor) {
        this.setPropertyExtractor(propertyExtractor);
    }

    /**
     * Primary constructor.
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects
     * @param propertyDefs container property definitions
     * @throws IllegalArgumentException if either parameter is null
     */
    protected AbstractQueryContainer(PropertyExtractor<? super T> propertyExtractor,
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

// Subclass hooks and methods

    /**
     * Perform a query to generate the list of Java objects that back this container.
     */
    protected abstract List<T> query();

    /**
     * Determine if the given list can still be used or not.
     */
    protected abstract boolean validate(List<T> list);

    /**
     * Invalidate the current query list, if any.
     */
    protected void invalidate() {
        this.currentList = null;
    }

// Internal methods

    /**
     * Get the Java backing object at the given index in the list.
     *
     * @return backing object, or null if {@code index} is out of range
     */
    protected T getJavaObject(int index) {
        List<T> list = this.getList();
        if (index < 0 || index >= list.size())
            return null;
        return list.get(index);
    }

    /**
     * Get the query list, validating it and regenerating if necessary.
     */
    protected List<T> getList() {
        if (this.currentList == null || !this.validate(this.currentList))
            this.currentList = this.query();
        return this.currentList;
    }

// Container

    @Override
    public SimpleItem<T> getItem(Object itemId) {
        if (!(itemId instanceof Integer))
            return null;
        int index = ((Integer)itemId).intValue();
        T obj = this.getJavaObject(index);
        if (obj == null)
            return null;
        return new SimpleItem<T>(obj, this.propertyMap, this.propertyExtractor);
    }

    @Override
    public Collection<Integer> getItemIds() {
        return new IntList(this.getList().size());
    }

    @Override
    public Set<String> getContainerPropertyIds() {
        return Collections.unmodifiableSet(this.propertyMap.keySet());
    }

    @Override
    public Property getContainerProperty(Object itemId, Object propertyId) {
        SimpleItem<T> item = this.getItem(itemId);
        return item != null ? item.getItemProperty(propertyId) : null;
    }

    @Override
    public Class<?> getType(Object propertyId) {
        PropertyDef<?> propertyDef = this.propertyMap.get(propertyId);
        return propertyDef != null ? propertyDef.getType() : null;
    }

    @Override
    public int size() {
        return this.getList().size();
    }

    @Override
    public boolean containsId(Object itemId) {
        if (!(itemId instanceof Integer))
            return false;
        int index = ((Integer)itemId).intValue();
        return index >= 0 && index < this.getList().size();
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public Item addItem(Object itemId) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public Item addItem() {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public boolean removeItem(Object itemId) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public boolean addContainerProperty(Object propertyId, Class<?> type, Object defaultValue) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public boolean removeContainerProperty(Object propertyId) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public boolean removeAllItems() {
        throw new UnsupportedOperationException();
    }

// Container.Indexed

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public Object addItemAt(int index) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public Item addItemAt(int index, Object newItemId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getIdByIndex(int index) {
        return index;
    }

    @Override
    public int indexOfId(Object itemId) {
        if (!(itemId instanceof Integer))
            return -1;
        int index = ((Integer)itemId).intValue();
        List<T> list = this.getList();
        if (index < 0 || index >= list.size())
            return -1;
        return index;
    }

// Container.Ordered

    @Override
    public Integer nextItemId(Object itemId) {
        if (!(itemId instanceof Integer))
            return null;
        int index = ((Integer)itemId).intValue();
        List<T> list = this.getList();
        if (index < 0 || index + 1 >= list.size())
            return null;
        return index + 1;
    }

    @Override
    public Integer prevItemId(Object itemId) {
        if (!(itemId instanceof Integer))
            return null;
        int index = ((Integer)itemId).intValue();
        List<T> list = this.getList();
        if (index - 1 < 0 || index >= list.size())
            return null;
        return index - 1;
    }

    @Override
    public Integer firstItemId() {
        return this.getList().isEmpty() ? null : 0;
    }

    @Override
    public Integer lastItemId() {
        List<T> list = this.getList();
        return list.isEmpty() ? null : list.size() - 1;
    }

    @Override
    public boolean isFirstId(Object itemId) {
        if (!(itemId instanceof Integer))
            return false;
        int index = ((Integer)itemId).intValue();
        return !this.getList().isEmpty() && index == 0;
    }

    @Override
    public boolean isLastId(Object itemId) {
        if (!(itemId instanceof Integer))
            return false;
        int index = ((Integer)itemId).intValue();
        return index == this.getList().size() - 1;
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public Item addItemAfter(Object previousItemId) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public Item addItemAfter(Object previousItemId, Object newItemId) {
        throw new UnsupportedOperationException();
    }

// Container.PropertySetChangeNotifier

    @Override
    public void addListener(Container.PropertySetChangeListener listener) {
        super.addListener(listener);
    }

    @Override
    public void removeListener(Container.PropertySetChangeListener listener) {
        super.removeListener(listener);
    }

// Container.ItemSetChangeNotifier

    @Override
    public void addListener(Container.ItemSetChangeListener listener) {
        super.addListener(listener);
    }

    @Override
    public void removeListener(Container.ItemSetChangeListener listener) {
        super.removeListener(listener);
    }

// IntList

    private static class IntList extends AbstractList<Integer> {

        private final int max;

        public IntList(int max) {
            if (max < 0)
                throw new IllegalArgumentException("max < 0");
            this.max = max;
        }

        @Override
        public int size() {
            return this.max;
        }

        @Override
        public Integer get(int index) {
            if (index < 0 || index >= this.max)
                throw new IndexOutOfBoundsException();
            return index;
        }
    }
}

