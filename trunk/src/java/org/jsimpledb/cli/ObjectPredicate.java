
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.base.Predicate;

import java.util.List;
import java.util.NavigableSet;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.util.ParseContext;

public class ObjectPredicate {

    public ObjectPredicate(List<ObjType> objTypes) {
    }

    public NavigableSet<ObjId> getIndexSet() {
        throw new UnsupportedOperationException();
    }

    public Predicate<ObjId> parse(Session session, ParseContext ctx) {
        throw new UnsupportedOperationException();
    }
}

