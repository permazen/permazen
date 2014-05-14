
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

public interface Action {

    /**
     * Perform some action on the session while a transaction is open.
     */
    void run(Session session) throws Exception;
}

