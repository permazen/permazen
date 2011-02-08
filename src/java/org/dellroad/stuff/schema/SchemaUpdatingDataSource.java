
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
 * the database schema using a {@link SchemaUpdater} on first access.
 *
 * @see SchemaUpdater
 */
public class SchemaUpdatingDataSource extends AbstractUpdatingDataSource {

    private SchemaUpdater schemaUpdater;

    /**
     * Configure the {@link SchemaUpdater} that will initialize and update the database. Required property.
     */
    public void setSchemaUpdater(SchemaUpdater schemaUpdater) {
        this.schemaUpdater = schemaUpdater;
    }

    @Override
    protected void updateDataSource(DataSource dataSource) throws SQLException {
        if (this.schemaUpdater == null)
            throw new IllegalArgumentException("no SchemaUpdater configured");
        this.schemaUpdater.initializeAndUpdateDatabase(dataSource);
    }
}

