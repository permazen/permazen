
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Database modification interface.
 */
public interface SchemaModification {

    /**
     * Apply this modification.
     *
     * @param c connection to the database
     * @throws SQLException if the update fails
     */
    void apply(Connection c) throws SQLException;
}

