
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import java.util.List;

public interface ActionListBuilder<T> {

    /**
     * Build list of actions for the given target.
     *
     * @return list of actions; null is equivalent to an empty list
     */
    List<? extends Action> buildActionList(T target);
}

