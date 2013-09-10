
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.util.ArrayDeque;
import java.util.IdentityHashMap;

/**
 * A registry for copies of {@link GraphCloneable} objects used during <i>graph cloning</i> operations.
 * A <i>graph clone</i> of a graph of objects is a deep copy without duplicates and preserving reference topology.
 *
 * <p>
 * See {@link GraphCloneable} for a complete description.
 * </p>
 *
 * @see GraphCloneable
 */
public class GraphCloneRegistry {

    private final IdentityHashMap<Object, Object> map = new IdentityHashMap<Object, Object>();

    private final ArrayDeque<GraphCloneable> stack = new ArrayDeque<GraphCloneable>();

    /**
     * Get the unique clone of a given value during a graph clone operation, creating it if necessary.
     *
     * <p>
     * If the {@code value} has already been cloned and registered with this instance, it is returned.
     * Otherwise, a new clone of {@code value} is created and registered with this instance by invoking
     * {@link GraphCloneable#createGraphClone value.createGraphClone()}, and then returned.
     * </p>
     *
     * <p>
     * If the {@code value} is null, null is returned.
     * <p>
     *
     * @param value original value
     * @return unique clone of {@code value}, or null if {@code value} is null
     * @throws IllegalStateException if {@link GraphCloneable#createGraphClone value.createGraphClone()}
     *  fails to invoke {@link #setGraphClone setGraphClone()}
     * @throws IllegalStateException if {@link GraphCloneable#createGraphClone value.createGraphClone()}
     *  invokes {@link #setGraphClone setGraphClone()} more than once
     * @throws IllegalStateException if {@link GraphCloneable#createGraphClone value.createGraphClone()}
     *  fails to invoke {@link #setGraphClone setGraphClone()} prior to recursing on other {@link GraphCloneable} fields
     */
    @SuppressWarnings("unchecked")
    public <T extends GraphCloneable> T getGraphClone(final T value) {

        // Verify parent clone registered itself before recursing on children
        final GraphCloneable parent = this.stack.peekFirst();
        if (parent != null && !this.map.containsKey(parent))
            throw new IllegalStateException("must invoke setGraphClone() prior to recursing via getGraphClone()");

        // Handle null
        if (value == null)
            return null;

        // Have we already cloned the value?
        if (this.map.containsKey(value))
            return (T)this.map.get(value);

        // Need to recurse
        this.stack.addFirst(value);
        try {

            // Create new clone
            try {
                value.createGraphClone(this);
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }

            // Verify it was registered
            if (!this.map.containsKey(value))
                throw new IllegalStateException("createGraphClone() failed to invoke setGraphClone() for value " + value);

            // Return clone
            return (T)this.map.get(value);
        } finally {
            this.stack.removeFirst();
        }
    }

    /**
     * Register the given clone as the unique instance associated with the object currently being cloned.
     *
     * <p>
     * The "object currently being cloned" is the {@link GraphCloneable} object associated with the top-most invocation
     * of {@link GraphCloneable#createGraphClone GraphCloneable.createGraphClone()} on the Java execution stack.
     * </p>
     *
     * <p>
     * This method should only be invoked within implementations of
     * {@link GraphCloneable#createGraphClone GraphCloneable.createGraphClone()}.
     * </p>
     *
     * @param clone the clone associated with the current object being graph cloned; may be null (but that would be weird)
     * @throws IllegalStateException if the current thread is not executing within
     *  an invocation of {@link GraphCloneable#createGraphClone GraphCloneable.createGraphClone()}
     */
    public void setGraphClone(GraphCloneable clone) {

        // Sanity check
        final GraphCloneable current = this.stack.peekFirst();
        if (current == null)
            throw new IllegalStateException("not executing within an invocation of GraphCloneable.createGraphClone()");
        if (this.map.containsKey(current))
            throw new IllegalStateException("duplicate invocation of setGraphClone()");

        // Register clone
        this.map.put(current, clone);
    }
}

