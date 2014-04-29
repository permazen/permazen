
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Support superclass for simple read-only, in-memory {@link Container} implementations where each
 * {@link Item} in the container is backed by a Java object.
 *
 * <p>
 * This {@link Container}'s {@link Property}s are defined via {@link PropertyDef}s, and a {@link PropertyExtractor}
 * is used to actually extract the property values from each underlying object (alternately, subclasses can override
 * {@link #getPropertyValue getPropertyValue()}). However, the easist way to configure the container {@link Property}s
 * is to pass a {@link ProvidesProperty &#64;ProvidesProperty}-annotated Java class to the {@link #AbstractSimpleContainer(Class)}
 * constructor.
 * </p>
 *
 * <p>
 * May be optionally configured with an {@link ExternalPropertyRegistry}; if so, this container's {@link Item} properties
 * will be {@link ExternalProperty}s and they will be registered with it. Subclasses can control this behavior by
 * overriding {@link #createBackedItem createBackedItem()}.
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
public abstract class AbstractSimpleContainer<I, T> extends AbstractInMemoryContainer<I, String, BackedItem<T>>
  implements PropertyExtractor<T>, Container.Filterable, Container.SimpleFilterable, Container.Sortable, Connectable {

    private final HashMap<String, PropertyDef<?>> propertyMap = new HashMap<String, PropertyDef<?>>();
    private PropertyExtractor<? super T> propertyExtractor;
    private ExternalPropertyRegistry registry;

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * After using this constructor, subsequent invocations of {@link #setPropertyExtractor setPropertyExtractor()}
     * and {@link #setProperties setProperties()} are required to define the properties of this container
     * and how to extract them.
     * </p>
     */
    protected AbstractSimpleContainer() {
        this((PropertyExtractor<? super T>)null);
    }

    /**
     * Constructor.
     *
     * <p>
     * After using this constructor, a subsequent invocation of {@link #setProperties setProperties()} is required
     * to define the properties of this container.
     * </p>
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects;
     *  may be null but then container is not usable until one is configured via
     * {@link #setPropertyExtractor setPropertyExtractor()}
     */
    protected AbstractSimpleContainer(PropertyExtractor<? super T> propertyExtractor) {
        this(propertyExtractor, null);
    }

    /**
     * Constructor.
     *
     * <p>
     * After using this constructor, a subsequent invocation of {@link #setPropertyExtractor setPropertyExtractor()} is required
     * to define how to extract the properties of this container; alternately, subclasses can override
     * {@link #getPropertyValue getPropertyValue()}.
     * </p>
     *
     * @param propertyDefs container property definitions; null is treated like the empty set
     */
    protected AbstractSimpleContainer(Collection<? extends PropertyDef<?>> propertyDefs) {
        this(null, propertyDefs);
    }

    /**
     * Constructor.
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects;
     *  may be null but then container is not usable until one is configured via
     * {@link #setPropertyExtractor setPropertyExtractor()}
     * @param propertyDefs container property definitions; null is treated like the empty set
     */
    protected AbstractSimpleContainer(PropertyExtractor<? super T> propertyExtractor,
      Collection<? extends PropertyDef<?>> propertyDefs) {
        this.setItemSorter(new SimpleItemSorter());
        this.setPropertyExtractor(propertyExtractor);
        this.setProperties(propertyDefs);
    }

    /**
     * Constructor.
     *
     * <p>
     * Properties will be determined by the {@link ProvidesProperty &#64;ProvidesProperty} and
     * {@link ProvidesPropertySort &#64;ProvidesPropertySort} annotated methods in the given class.
     * </p>
     *
     * @param type class to introspect for annotated methods
     * @throws IllegalArgumentException if {@code type} is null
     * @throws IllegalArgumentException if {@code type} has two {@link ProvidesProperty &#64;ProvidesProperty}
     *  or {@link ProvidesPropertySort &#64;ProvidesPropertySort} annotated methods for the same property
     * @throws IllegalArgumentException if a {@link ProvidesProperty &#64;ProvidesProperty}-annotated method with no
     *  {@linkplain ProvidesProperty#value property name specified} has a name which cannot be interpreted as a bean
     *  property "getter" method
     * @see ProvidesProperty
     * @see ProvidesPropertySort
     * @see ProvidesPropertyScanner
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected AbstractSimpleContainer(Class<? super T> type) {
        // Why the JLS forces this stupid cast:
        //  http://stackoverflow.com/questions/4902723/why-cant-a-java-type-parameter-have-a-lower-bound
        final ProvidesPropertyScanner<? super T> propertyReader
          = (ProvidesPropertyScanner<? super T>)new ProvidesPropertyScanner(type);
        this.setItemSorter(new SimpleItemSorter());
        this.setPropertyExtractor(propertyReader.getPropertyExtractor());
        this.setProperties(propertyReader.getPropertyDefs());
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
     * @param propertyExtractor used to extract properties from the underlying Java objects;
     *  may be null but the container is not usable without one
     */
    public void setPropertyExtractor(PropertyExtractor<? super T> propertyExtractor) {
        this.propertyExtractor = propertyExtractor;
    }

    /**
     * Read the value of the property defined by {@code propertyDef} from the given object.
     *
     * <p>
     * The implementation in {@link AbstractSimpleContainer} just delegates to the {@linkplain #setPropertyExtractor configured}
     * {@link PropertyExtractor}; subclasses may override to customize property extraction.
     * </p>
     *
     * @param obj Java object
     * @param propertyDef definition of which property to read
     * @throws NullPointerException if either parameter is null
     * @throws IllegalStateException if no {@link PropertyExtractor} is configured for this container
     */
    @Override
    public <V> V getPropertyValue(T obj, PropertyDef<V> propertyDef) {
        if (this.propertyExtractor == null)
            throw new IllegalStateException("no PropertyExtractor is configured for this container");
        return this.propertyExtractor.getPropertyValue(obj, propertyDef);
    }

    /**
     * Change the configured properties of this container.
     *
     * @param propertyDefs container property definitions; null is treated like the empty set
     * @throws IllegalArgumentException if {@code propertyDefs} contains a property with a duplicate name
     */
    public void setProperties(Collection<? extends PropertyDef<?>> propertyDefs) {
        if (propertyDefs == null)
            propertyDefs = Collections.<PropertyDef<?>>emptySet();
        this.propertyMap.clear();
        for (PropertyDef<?> propertyDef : propertyDefs) {
            if (this.propertyMap.put(propertyDef.getName(), propertyDef) != null)
                throw new IllegalArgumentException("duplicate property name `" + propertyDef.getName() + "'");
        }
        this.fireContainerPropertySetChange();
    }

    /**
     * Add or replace a configured property of this container.
     *
     * @param propertyDef new container property definitions
     * @throws IllegalArgumentException if {@code propertyDef} is null
     */
    public void setProperty(PropertyDef<?> propertyDef) {
        if (propertyDef == null)
            throw new IllegalArgumentException("null propertyDef");
        this.propertyMap.put(propertyDef.getName(), propertyDef);
        this.fireContainerPropertySetChange();
    }

    /**
     * Get the {@link ExternalPropertyRegistry} associated with this instance, if any.
     */
    public ExternalPropertyRegistry getExternalPropertyRegistry() {
        return this.registry;
    }

    /**
     * Configure a {@link ExternalPropertyRegistry} for this instance. This will cause {@link #createBackedItem createBackedItem()}
     * to create {@link BackedExternalItem}s associated with {@code registry}.
     *
     * @param registry registry for item properties, or null for none
     */
    public void setExternalPropertyRegistry(ExternalPropertyRegistry registry) {
        this.registry = registry;
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
            final BackedItem<T> item = this.createBackedItem(obj, this.propertyMap, this);
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
     * </p>
     *
     * <p>
     * This method is not used by this class but is defined as a convenience for subclasses.
     * </p>
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
     * </p>
     *
     * <p>
     * This method is not used by this class but is defined as a convenience for subclasses.
     * </p>
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

// Connectable

    /**
     * Connect this instance to non-Vaadin resources.
     *
     * <p>
     * The implementation in {@link AbstractSimpleContainer} does nothing.
     * </p>
     *
     * @throws IllegalStateException if there is no {@link com.vaadin.server.VaadinSession} associated with the current thread
     */
    @Override
    public void connect() {
    }

    /**
     * Disconnect this instance from non-Vaadin resources.
     *
     * <p>
     * The implementation in {@link AbstractSimpleContainer} does nothing.
     * </p>
     *
     * @throws IllegalStateException if there is no {@link com.vaadin.server.VaadinSession} associated with the current thread
     */
    @Override
    public void disconnect() {
    }

// Container and superclass required methods

    // Workaround for http://dev.vaadin.com/ticket/8856
    @Override
    @SuppressWarnings("unchecked")
    public List<I> getItemIds() {
        return (List<I>)super.getItemIds();
    }

    @Override
    public Set<String> getContainerPropertyIds() {
        return Collections.unmodifiableSet(this.propertyMap.keySet());
    }

    @Override
    public Property<?> getContainerProperty(Object itemId, Object propertyId) {
        final BackedItem<T> entityItem = this.getItem(itemId);
        if (entityItem == null)
            return null;
        return entityItem.getItemProperty(propertyId);
    }

    @Override
    public Class<?> getType(Object propertyId) {
        final PropertyDef<?> propertyDef = this.propertyMap.get(propertyId);
        return propertyDef != null ? propertyDef.getType() : null;
    }

    @Override
    public BackedItem<T> getUnfilteredItem(Object itemId) {
        final T obj = this.getJavaObject(itemId);
        if (obj == null)
            return null;
        return this.createBackedItem(obj, this.propertyMap, this);
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
     * </p>
     */
    protected void afterReload() {
    }

    /**
     * Create a {@link BackedItem} for the given backing Java object.
     *
     * <p>
     * The implementation in {@link AbstractSimpleContainer} returns
     * {@code new BackedExternalItem<T>(this.registry, object, propertyMap, propertyExtractor)} if this instance has a
     * configured {@link ExternalPropertyRegistry}, otherwise {@code new SimpleItem<T>(object, propertyMap, propertyExtractor)}.
     * </p>
     *
     * @param object underlying Java object
     * @param propertyMap mapping from property name to property definition
     * @param propertyExtractor extracts the property value from {@code object}
     * @throws IllegalArgumentException if any parameter is null
     */
    protected BackedItem<T> createBackedItem(T object, Map<String, PropertyDef<?>> propertyMap,
      PropertyExtractor<? super T> propertyExtractor) {
        return this.registry != null ?
          new BackedExternalItem<T>(this.registry, object, propertyMap, propertyExtractor) :
          new SimpleItem<T>(object, propertyMap, propertyExtractor);
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
    public Collection<Container.Filter> getContainerFilters() {
        return super.getContainerFilters();
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
        final ArrayList<String> propertyIds = new ArrayList<String>(this.propertyMap.size());
        for (Map.Entry<String, PropertyDef<?>> entry : this.propertyMap.entrySet()) {
            if (this.propertyExtractor instanceof SortingPropertyExtractor) {
                final SortingPropertyExtractor<? super T> sortingPropertyExtractor
                  = (SortingPropertyExtractor<? super T>)this.propertyExtractor;
                if (sortingPropertyExtractor.canSort(entry.getValue())) {
                    propertyIds.add(entry.getKey());
                    continue;
                }
            }
            if (entry.getValue().isSortable())
                propertyIds.add(entry.getKey());
        }
        return propertyIds;
    }

// ItemSorter class

    /**
     * {@link ItemSorter} implementation used by {@link AbstractSimpleContainer}.
     */
    private class SimpleItemSorter extends DefaultItemSorter {

        @Override
        @SuppressWarnings("unchecked")
        protected int compareProperty(Object propertyId, boolean ascending, Item item1, Item item2) {

            // Get property definition
            final PropertyDef<?> propertyDef = AbstractSimpleContainer.this.propertyMap.get(propertyId);
            if (propertyDef == null)
                return super.compareProperty(propertyId, ascending, item1, item2);

            // Ask SortingPropertyExtractor if we have one
            if (AbstractSimpleContainer.this.propertyExtractor instanceof SortingPropertyExtractor) {
                final SortingPropertyExtractor<? super T> sortingPropertyExtractor
                  = (SortingPropertyExtractor<? super T>)AbstractSimpleContainer.this.propertyExtractor;
                if (sortingPropertyExtractor.canSort(propertyDef)) {
                    final T obj1 = ((BackedItem<T>)item1).getObject();
                    final T obj2 = ((BackedItem<T>)item2).getObject();
                    final int diff = sortingPropertyExtractor.sort(propertyDef, obj1, obj2);
                    return ascending ? diff : -diff;
                }
            }

            // Ask property definition
            if (propertyDef.isSortable()) {
                final int diff = this.sort(propertyDef, item1, item2);
                return ascending ? diff : -diff;
            }

            // Defer to superclass
            return super.compareProperty(propertyId, ascending, item1, item2);
        }

        // This method exists only to allow the generic parameter <V> to be bound
        private <V> int sort(PropertyDef<V> propertyDef, Item item1, Item item2) {
            return propertyDef.sort(propertyDef.read(item1), propertyDef.read(item2));
        }
    }
}

