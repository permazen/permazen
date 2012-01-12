
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

/**
 * Database action interface.
 *
 * @param <T> database transaction type
 */
public interface DatabaseAction<T> {

    /**
     * Apply this action to the database using the provided open transaction.
     *
     * @param transaction open transaction
     * @throws Exception if the action fails
     */
    void apply(T transaction) throws Exception;
}

