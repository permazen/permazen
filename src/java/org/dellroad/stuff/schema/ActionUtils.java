
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
 * Database utility methods.
 */
public final class ActionUtils {

    private ActionUtils() {
    }

    /**
     * Apply a {@link DatabaseAction} to a {@link DataSource}.
     *
     * <p>
     * This implementation simply invokes {@link DatabaseAction#apply} after acquiring
     * a {@link Connection} from the given {@link DataSource} (and closing it in any event when done).
     */
    public static void applyAction(DataSource dataSource, DatabaseAction action) throws SQLException {
        Connection c = dataSource.getConnection();
        try {
            action.apply(c);
        } finally {
            c.close();
        }
    }

    /**
     * Apply a {@link DatabaseAction} to a {@link DataSource} within a transaction.
     *
     * <p>
     * This implementation does the standard JDBC thing using serializable isolation.
     *
     * @see TransactionalDatabaseAction
     */
    public static void applyInTransaction(DataSource dataSource, DatabaseAction action) throws SQLException {
        ActionUtils.applyAction(dataSource, new TransactionalDatabaseAction(action));
    }
}

