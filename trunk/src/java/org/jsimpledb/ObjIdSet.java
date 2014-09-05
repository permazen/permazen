
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.collect.ForwardingSet;

import java.util.HashSet;
import java.util.Set;

import org.jsimpledb.core.ObjId;

/**
 * A set of {@link ObjId}s.
 *
 * <p>
 * Typically used when performing a copy operation of a (possibly) cyclic graph of object references,
 * to keep track of which objects have already been copied.
 * </p>
 *
 * @see JObject#copyTo
 */
public class ObjIdSet extends ForwardingSet<ObjId> {

    private final HashSet<ObjId> objects = new HashSet<ObjId>(); // TODO: use a more efficient data structure with longs

    @Override
    protected Set<ObjId> delegate() {
        return this.objects;
    }
}

