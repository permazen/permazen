
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.NavigableSet;

import org.jsimpledb.core.ObjId;

public abstract class ObjectChannel extends AbstractChannel<ObjId> {

    protected ObjectChannel(Session session) {
        super(new ObjectItemType(session));
    }

    @Override
    public abstract NavigableSet<ObjId> getItems(Session session);
}

