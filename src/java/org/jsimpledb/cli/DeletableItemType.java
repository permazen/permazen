
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

public interface DeletableItemType<T> extends ItemType<T> {

    boolean delete(Session session, T item);
}

