
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import org.jsimpledb.change.Change;
import org.springframework.context.ApplicationEvent;

/**
 * Spring application event that notifies about a {@link Change} to the database.
 */
@SuppressWarnings("serial")
public class DataChangeEvent extends ApplicationEvent {

    private final Change<?> change;

    public DataChangeEvent(Object source, Change<?> change) {
        super(source);
        if (change == null)
            throw new IllegalArgumentException("null change");
        this.change = change;
    }

    public Change<?> getChange() {
        return this.change;
    }
}

