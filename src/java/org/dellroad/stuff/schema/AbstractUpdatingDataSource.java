
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
 * A {@link DataSource} that wraps an inner {@link DataSource} and automatically performs some update
 * operation on the inner {@link DataSource} on first access.
 *
 * <p>
 * The {@link #setDataSource dataSource} property is required.
 */
public abstract class AbstractUpdatingDataSource implements DataSource {

    private DataSource dataSource;
    private boolean updated;

    /**
     * Configure the underlying {@link DataSource}. Required property.
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Update the inner {@link DataSource}.
     *
     * <p>
     * This method will be invoked at most once.
     */
    protected abstract void updateDataSource(DataSource dataSource) throws SQLException;

    /**
     * Get the underlying {@link DataSource}.
     */
    protected DataSource getInnerDataSource() {
        return this.dataSource;
    }

    /**
     * Determine if the underlying {@link DataSource} has been updated.
     *
     * <p>
     * If the update is currently happening in another thread, the current thread will
     * block until the update operation finishes.
     */
    protected synchronized boolean isUpdated() {
        return this.updated;
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
            this.updateDataSource(this.dataSource);
            this.updated = true;
        }
        return this.dataSource;
    }
}

