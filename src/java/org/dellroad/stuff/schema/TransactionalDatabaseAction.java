
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Wraps an underlying {@link DatabaseAction} and applies it transactionally using serializable isolation.
 */
public class TransactionalDatabaseAction implements DatabaseAction {

    private final DatabaseAction action;

    public TransactionalDatabaseAction(DatabaseAction action) {
        if (action == null)
            throw new IllegalArgumentException("null action");
        this.action = action;
    }

    /**
     * Invoke this instance's wrapped {@link DatabaseAction} within an SQL transaction.
     */
    @Override
    public void apply(Connection c) throws SQLException {
        boolean success = false;
        try {
            c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            c.setAutoCommit(false);
            action.apply(c);
            c.commit();
            success = true;
        } finally {
            if (!success)
                c.rollback();
        }
    }
}

