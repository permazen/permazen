
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * A {@link DataSource} that wraps an inner {@link DataSource} and automatically intializes and updates
 * the database schema using a {@link SQLSchemaUpdater} on first access.
 *
 * @see SQLSchemaUpdater
 */
public class SchemaUpdatingDataSource extends AbstractUpdatingDataSource {

    private SQLSchemaUpdater schemaUpdater;

    /**
     * Configure the {@link SQLSchemaUpdater} that will initialize and update the database. Required property.
     */
    public void setSchemaUpdater(SQLSchemaUpdater schemaUpdater) {
        this.schemaUpdater = schemaUpdater;
    }

    @Override
    protected void updateDataSource(DataSource dataSource) throws SQLException {
        if (this.schemaUpdater == null)
            throw new IllegalArgumentException("no SchemaUpdater configured");
        try {
            this.schemaUpdater.initializeAndUpdateDatabase(dataSource);
        } catch (RuntimeException e) {
            if (e.getMessage() == null && e.getCause() instanceof SQLException)     // unwrap checked exception
                throw (SQLException)e.getCause();
            throw e;
        }
    }
}

