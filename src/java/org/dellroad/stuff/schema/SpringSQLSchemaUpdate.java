
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Spring-enabled version of {@link SQLSchemaUpdate}.
 *
 * <p>
 * This class provides the combined functionality of {@link SQLSchemaUpdate} and {@link AbstractSpringSchemaUpdate}.
 */
public class SpringSQLSchemaUpdate extends AbstractSpringSchemaUpdate {

    private SQLDatabaseAction action;

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (this.action == null)
            throw new Exception("no SQLDatabaseAction configured");
    }

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

