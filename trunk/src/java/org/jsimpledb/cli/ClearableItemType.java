
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

public interface ClearableItemType<T> extends ItemType<T> {

    void clear(Session session, T item);
}

