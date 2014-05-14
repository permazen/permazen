
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

/**
 * Data channel. Instances are not allowed to make any changes until {@link #getItems getItems()} is invoked.
 */
public interface Channel<T> {

    ItemType<T> getItemType();

    Iterable<T> getItems(Session session);
}

