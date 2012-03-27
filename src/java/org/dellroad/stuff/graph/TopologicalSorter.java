
/*
 * Copyright (C) 2008-2009 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Topological sorting utility class.
 */
public class TopologicalSorter<E> {

    private final Collection<E> nodes;
    private final EdgeLister<E> edgeLister;
    private final Comparator<? super E> tieBreaker;

    private HashMap<E, Boolean> visited;
    private ArrayList<E> ordering;

    /**
     * Primary constructor.
     *
     * @param nodes partially ordered nodes to be sorted
     * @param edgeLister provides the edges defining the partial order
     * @param tieBreaker used to sort nodes that are not otherwise ordered,
     *  or null to tie break based on the original ordering
     */
    public TopologicalSorter(Collection<E> nodes, EdgeLister<E> edgeLister, Comparator<? super E> tieBreaker) {
        this.nodes = nodes;
        this.edgeLister = edgeLister;
        if (tieBreaker == null)
            tieBreaker = getDefaultTieBreaker();
        this.tieBreaker = tieBreaker;
    }

    /**
     * Convenience constructor for when ties should be broken based on the original ordering.
     *
     * <p>
     * Equivalent to:
     *  <blockquote>
     *  {@code TopologicalSorter(nodes, edgeLister, null);}
     *  </blockquote>
     * </p>
     */
    public TopologicalSorter(Collection<E> nodes, EdgeLister<E> edgeLister) {
        this(nodes, edgeLister, null);
    }

    /**
     * Produce a total ordering of the nodes consistent with the partial ordering
     * implied by the edge lister and tie breaker provided to the constructor.
     *
     * <p>
     * The returned list will have the property that if there is an edge from X to Y,
     * then X will appear before Y in the list. If there is no edge (or sequence of edges) from X to Y
     * in either direction, then X will appear before Y if the tie breaker sorts X before Y.
     * </p>
     *
     * <p>
     * This implementation runs in linear time in the number of nodes and edges in the graph.
     * </p>
     *
     * @return sorted, mutable list of nodes
     * @throws IllegalArgumentException if the partial ordering relation contains a cycle
     */
    public List<E> sort() {

        // Order nodes according to reverse tie breaker ordering
        ArrayList<E> startList = Collections.list(Collections.enumeration(this.nodes));
        Collections.sort(startList, getTieBreaker(true));

        // Perform depth-first search through nodes
        this.visited = new HashMap<E, Boolean>(startList.size());
        this.ordering = new ArrayList<E>(startList.size());
        for (E node : startList)
            visit(node, true);

        // Reverse list
        Collections.reverse(this.ordering);
        return this.ordering;
    }

    /**
     * Same as {@link #sort sort()} but treats all edges as reversed.
     *
     * <p>
     * The returned list will have the property that if there is an edge from X to Y,
     * then Y will appear before X in the list. If there is no edge (or sequence of edges) from X to Y
     * in either direction, then X will appear before Y if the tie breaker sorts X before Y.
     * </p>
     *
     * @return sorted, mutable list of nodes
     * @throws IllegalArgumentException if the partial ordering relation contains a cycle
     */
    public List<E> sortEdgesReversed() {

        // Order nodes according to normal tie breaker ordering
        ArrayList<E> startList = Collections.list(Collections.enumeration(this.nodes));
        Collections.sort(startList, getTieBreaker(false));

        // Perform depth-first search through nodes
        this.visited = new HashMap<E, Boolean>(startList.size());
        this.ordering = new ArrayList<E>(startList.size());
        for (E node : startList)
            visit(node, false);

        // Done
        return this.ordering;
    }

    private void visit(E node, boolean reverse) {

        // Have we been here before?
        Boolean state = this.visited.get(node);
        if (state != null) {
            if (!state.booleanValue())
                throw new IllegalArgumentException("cycle in graph containing " + node);
            return;
        }
        this.visited.put(node, false);

        // Get all destination nodes of all out-edges
        ArrayList<E> targets = Collections.list(Collections.enumeration(this.edgeLister.getOutEdges(node)));

        // Sort them in reverse desired order and recurse
        Collections.sort(targets, getTieBreaker(reverse));
        for (E target : targets)
            visit(target, reverse);

        // Add this node to list in post-order and mark complete
        this.ordering.add(node);
        this.visited.put(node, true);
    }

    private Comparator<? super E> getDefaultTieBreaker() {
        final HashMap<E, Integer> orderMap = new HashMap<E, Integer>(this.nodes.size());
        int posn = 0;
        for (E node : this.nodes)
            orderMap.put(node, posn++);
        return new Comparator<E>() {
            public int compare(E node1, E node2) {
                return orderMap.get(node1) - orderMap.get(node2);
            }
        };
    }

    private Comparator<? super E> getTieBreaker(boolean reverse) {
        if (reverse)
            return Collections.reverseOrder(this.tieBreaker);
        return this.tieBreaker;
    }

    /**
     * Implemented by classes that can enumerate the outgoing edges from a node in a graph.
     */
    public interface EdgeLister<E> {

        /**
         * Get the set of all nodes X for which there is an edge from {@code node} to X.
         */
        Set<E> getOutEdges(E node);
    }
}

