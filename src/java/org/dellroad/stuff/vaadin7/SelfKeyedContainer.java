
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.util.Collection;

/**
 * A {@link SimpleKeyedContainer} where the item IDs are the underlying container objects themselves.
 *
 * <p>
 * Restriction: instances can't contain two objects that are {@link Object#equals equal()} to each other.
 * </p>
 *
 * @param <T> the type of the Java objects that back each {@link com.vaadin.data.Item} in the container
 */
@SuppressWarnings("serial")
public class SelfKeyedContainer<T> extends SimpleKeyedContainer<T, T> {

    /**
     * Constructor.
     *
     * <p>
     * After using this constructor, subsequent invocations of {@link #setPropertyExtractor setPropertyExtractor()}
     * and {@link #setProperties setProperties()} are required to define the properties of this container
     * and how to extract them.
     */
    public SelfKeyedContainer() {
    }

    /**
     * Constructor.
     *
     * <p>
     * After using this constructor, a subsequent invocation of {@link #setProperties setProperties()} is required
     * to define the properties of this container.
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects;
     *  may be null but then container is not usable until one is configured via
     * {@link #setPropertyExtractor setPropertyExtractor()}
     */
    public SelfKeyedContainer(PropertyExtractor<? super T> propertyExtractor) {
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
    protected SelfKeyedContainer(Collection<? extends PropertyDef<?>> propertyDefs) {
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
    public SelfKeyedContainer(PropertyExtractor<? super T> propertyExtractor, Collection<? extends PropertyDef<?>> propertyDefs) {
        super(propertyExtractor, propertyDefs);
    }

    /**
     * Constructor.
     *
     * <p>
     * Properties will be determined by the {@link ProvidesProperty &#64;ProvidesProperty}-annotated fields and
     * methods in the given class.
     * </p>
     *
     * @param type class to introspect for {@link ProvidesProperty &#64;ProvidesProperty}-annotated fields and methods
     * @throws IllegalArgumentException if {@code type} is null
     * @throws IllegalArgumentException if an annotated method with no {@linkplain ProvidesProperty#value property name specified}
     *  has a name which cannot be interpreted as a bean property "getter" method
     * @throws IllegalArgumentException if {@code type} has two {@link ProvidesProperty &#64;ProvidesProperty}-annotated
     *  fields or methods with the same {@linkplain ProvidesProperty#value property name}
     * @see PropertyReader
     */
    protected SelfKeyedContainer(Class<? super T> type) {
        super(type);
    }

    /**
     * Get the key to be used as item ID for the given object.
     *
     * <p>
     * The implementation in {@link SelfKeyedContainer} always returns {@code obj}.
     *
     * @param obj underlying container object
     * @return key for object
     */
    @Override
    protected T getKeyFor(T obj) {
        return obj;
    }
}

