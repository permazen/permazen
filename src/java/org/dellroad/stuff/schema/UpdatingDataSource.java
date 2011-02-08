
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * A {@link DataSource} that wraps an inner {@link DataSource} and automatically applies a configured
 * {@link DatabaseAction} on first access.
 *
 * @see DatabaseAction
 */
public class UpdatingDataSource extends AbstractUpdatingDataSource {

    private DatabaseAction action;
    private boolean transactional = true;

    /**
     * Configure the {@link DatabaseAction} that be applied to the database on first access. Required property.
     */
    public void setDatabaseAction(DatabaseAction action) {
        this.action = action;
    }

    /**
     * Configure whether the {@link DatabaseAction} is applied transactionally or not.
     * Default is {@code true}.
     */
    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    @Override
    protected void updateDataSource(DataSource dataSource) throws SQLException {
        if (this.action == null)
            throw new IllegalArgumentException("no DatabaseAction configured");
        if (this.transactional)
            ActionUtils.applyInTransaction(dataSource, this.action);
        else
            ActionUtils.applyAction(dataSource, this.action);
    }
}

