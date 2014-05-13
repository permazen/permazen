
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

public interface TransactionAction {

    /**
     * Perform some action while a transaction is open.
     */
    void run(Session session) throws Exception;
}

