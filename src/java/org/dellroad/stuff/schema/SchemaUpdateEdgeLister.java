
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.util.Set;

import org.dellroad.stuff.graph.TopologicalSorter.EdgeLister;

/**
 * {@link EdgeLister} implementation reflecting {@link SchemaUpdate} predecessor constraints.
 * Graph edges will exist from each update to its predecessors. Note, this is the reverse of the actual
 * desired ordering of the updates.
 */
public class SchemaUpdateEdgeLister implements EdgeLister<SchemaUpdate> {

    @Override
    public Set<SchemaUpdate> getOutEdges(SchemaUpdate update) {
        return update.getRequiredPredecessors();
    }
}

