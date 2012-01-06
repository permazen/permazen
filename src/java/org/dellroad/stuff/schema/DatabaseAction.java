
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

/**
 * Database action interface.
 *
 * @param <C> database connection type
 */
public interface DatabaseAction<C> {

    /**
     * Apply this action to the database via the provided connection.
     *
     * @param connection connection to the database
     * @throws Exception if the action fails
     */
    void apply(C connection) throws Exception;
}

