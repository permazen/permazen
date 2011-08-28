
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.util;

import java.util.List;
import java.util.Queue;

/**
 * Interface for linked lists. This interface is missing from the Java collections API.
 */
public interface LinkedList<E> extends List<E>, Queue<E> {

    void addFirst(E obj);
    void addLast(E obj);
    E getFirst();
    E getLast();
    E removeFirst();
    E removeLast();
}

