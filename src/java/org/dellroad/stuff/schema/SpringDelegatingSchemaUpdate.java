
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Spring-enabled version of {@link DelegatingSchemaUpdate}.
 *
 * <p>
 * This class provides the combined functionality of {@link DelegatingSchemaUpdate} and {@link AbstractSpringSchemaUpdate}.
 */
public class SpringDelegatingSchemaUpdate extends AbstractSpringSchemaUpdate {

    private DatabaseAction action;

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (this.action == null)
            throw new Exception("no DatabaseAction configured");
    }

    /**
     * Configure the {@link DatabaseAction}. This is a required property.
     *
     * @see DatabaseAction
     */
    public void setDatabaseAction(DatabaseAction action) {
        this.action = action;
    }

    @Override
    public void apply(Connection c) throws SQLException {
        this.action.apply(c);
    }
}

