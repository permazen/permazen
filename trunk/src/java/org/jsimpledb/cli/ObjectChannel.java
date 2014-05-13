
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.List;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;

public interface ObjectChannel extends Channel<ObjId> {

    /**
     * Get the set of object types which can possibly appear in this channel.
     */
    List<ObjType> getObjTypes();
}

