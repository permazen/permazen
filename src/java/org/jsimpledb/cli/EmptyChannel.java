
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Collections;

public class EmptyChannel extends AbstractChannel<Void> {

    public EmptyChannel() {
        super(Void.class);
    }

    @Override
    public final Iterable<Void> getItems(Session session) {
        this.process(session);
        return Collections.<Void>emptySet();
    }

    protected void process(Session session) {
    }
}

