
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

public class ObjectPredicate implements Parser<Predicate<ObjId>> {

    public ObjectPredicate(List<ObjType> objTypes) {
    }

    public NavigableSet<ObjId> getIndexSet() {
        return null;
    }

    @Override
    public Predicate<ObjId> parse(Session session, Channels channels, ParseContext ctx) {
        return null;
    }
}

