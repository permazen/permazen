
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Database action interface.
 */
public interface DatabaseAction {

    /**
     * Apply this action to the database via the provided {@link Connection}.
     *
     * @param c connection to the database
     * @throws SQLException if the update fails
     */
    void apply(Connection c) throws SQLException;
}

