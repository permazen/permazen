
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * A {@link DataSource} that wraps an inner {@link DataSource} and automatically applies a configured
 * {@link SQLCommandList} on first access.
 *
 * @see SQLCommandList
 */
public class UpdatingDataSource extends AbstractUpdatingDataSource {

    private SQLCommandList action;
    private boolean transactional = true;

    /**
     * Configure the {@link SQLCommandList} to be applied to the database on first access. Required property.
     */
    public void setSQLCommandList(SQLCommandList action) {
        this.action = action;
    }

    /**
     * Configure whether the {@link SQLCommandList} is applied transactionally or not.
     * Default is {@code true}.
     */
    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    @Override
    protected void updateDataSource(DataSource dataSource) throws SQLException {

        // Sanity check
        if (this.action == null)
            throw new IllegalArgumentException("no SQLCommandList configured");

        // Get connection
        Connection c = dataSource.getConnection();
        boolean tx = this.transactional;
        try {
            try {

                // Open transaction if so configured
                if (tx) {
                    c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                    c.setAutoCommit(false);
                }

                // Apply SQL command(s)
                this.action.apply(c);

                // Commit transaction
                if (tx)
                    c.commit();
                tx = false;
            } finally {
                if (tx)
                    c.rollback();
            }
        } finally {
            c.close();
        }
    }
}

