
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * A {@link DataSource} that wraps an inner {@link DataSource} and automatically intializes and updates
 * the database schema using a {@link SchemaUpdater} on first access.
 *
 * @see SchemaUpdater
 */
public class SchemaUpdatingDataSource implements DataSource {

    private DataSource dataSource;
    private SchemaUpdater schemaUpdater;
    private boolean updated;

    /**
     * Configure the underlying {@link DataSource}. Required property.
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Configure the {@link SchemaUpdater} that will initialize and update the database. Required property.
     */
    public void setSchemaUpdater(SchemaUpdater schemaUpdater) {
        this.schemaUpdater = schemaUpdater;
    }

    // DataSource methods

    @Override
    public Connection getConnection() throws SQLException {
        return getUpdatedDataSource().getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getUpdatedDataSource().getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return getUpdatedDataSource().getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter pw) throws SQLException {
        getUpdatedDataSource().setLogWriter(pw);
    }

    @Override
    public void setLoginTimeout(int timeout) throws SQLException {
        getUpdatedDataSource().setLoginTimeout(timeout);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return getUpdatedDataSource().getLoginTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> cl) throws SQLException {
        return cl.cast(getUpdatedDataSource());
    }

    @Override
    public boolean isWrapperFor(Class<?> cl) throws SQLException {
        return cl.isInstance(getUpdatedDataSource());
    }

    // Internal methods

    private synchronized DataSource getUpdatedDataSource() throws SQLException {
        if (!this.updated) {
            if (this.dataSource == null)
                throw new IllegalArgumentException("no DataSource configured");
            if (this.schemaUpdater == null)
                throw new IllegalArgumentException("no SchemaUpdater configured");
            this.schemaUpdater.initializeAndUpdateDatabase(this.dataSource);
            this.updated = true;
        }
        return this.dataSource;
    }
}

