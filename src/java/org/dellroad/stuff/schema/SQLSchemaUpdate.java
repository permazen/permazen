
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A {@link SchemaUpdate} that performs its update using a {@link SQLDatabaseAction}.
 */
public class SQLSchemaUpdate extends AbstractSchemaUpdate {

    private SQLDatabaseAction action;

    /**
     * Configure the {@link SQLDatabaseAction}. This is a required property.
     *
     * @see SQLDatabaseAction
     */
    public void setSQLDatabaseAction(SQLDatabaseAction action) {
        this.action = action;
    }

    @Override
    public void apply(Connection c) throws SQLException {
        if (this.action == null)
            throw new IllegalArgumentException("no SQLDatabaseAction configured");
        this.action.apply(c);
    }
}

