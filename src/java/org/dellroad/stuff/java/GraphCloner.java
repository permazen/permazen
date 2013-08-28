
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.util.ArrayList;
import java.util.IdentityHashMap;

/**
 * Creates <i>graph clones</i>, i.e., deep copies of object graphs preserving reference topology.
 *
 * <p>
 * See {@link GraphClonable} for a complete description.
 * </p>
 *
 * @see GraphClonable
 */
public final class GraphCloner {

    private static final ThreadLocalHolder<GraphCloner> CURRENT = new ThreadLocalHolder<GraphCloner>();

    private final IdentityHashMap<Object, Object> map = new IdentityHashMap<Object, Object>();

    private GraphCloner() {
    }

    /**
     * Get or create the unique clone of a given value during a graph clone operation.
     *
     * <p>
     * If the value has already been cloned, this returns the existing graph clone of {@code value}.
     * Otherwise, it creates a new clone, associates it with the current thread, and returns it.
     * </p>
     *
     * <p>
     * If the {@code value} is null, null is returned.
     * <p>
     *
     * @param value original value
     * @return unique clone of {@code value}, or null if {@code value} is null
     */
    @SuppressWarnings("unchecked")
    public static <T extends GraphClonable<T>> T getGraphClone(final T value) {

        // Handle null
        if (value == null)
            return null;

        // Get current thread's tracker
        final GraphCloner tracker = CURRENT.get();

        // If not set, this is the top of the object graph, so create a new cloner and try again
        if (tracker == null) {
            final ArrayList<T> result = new ArrayList<T>(1);
            CURRENT.invoke(new GraphCloner(), new Runnable() {
                @Override
                public void run() {
                    result.add(GraphCloner.getGraphClone(value));
                }
            });
            return result.get(0);
        }

        // Have we already cloned this value?
        T clone = (T)tracker.map.get(value);
        if (clone != null)
            return clone;

        // Create new clone
        clone = value.createGraphClone();

        // Register it immediately so we can't create more than one
        tracker.map.put(value, clone);

        // Graph clone references
        value.copyGraphClonables(clone);
        return clone;
    }
}

