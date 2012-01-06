
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An SQL {@link DatabaseAction} that executes a single SQL statement.
 */
public class SQLCommand implements DatabaseAction<Connection> {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final String sql;

    /**
     * Constructor.
     *
     * @param sql the SQL to execute; must be a single statement
     * @throws IllegalArgumentException if {@code sql} is null or contains only whitespace
     */
    public SQLCommand(String sql) {
        if (sql == null)
            throw new IllegalArgumentException("null sql");
        sql = sql.trim();
        if (sql.length() == 0)
            throw new IllegalArgumentException("empty sql");
        this.sql = sql;
    }

    public String getSQL() {
        return this.sql;
    }

    /**
     * Execute the SQL statement.
     *
     * <p>
     * The implementation in {@link SQLCommand} creates a {@link Statement} and then executes the configured
     * SQL command via {@link Statement#execute}. Subclasses may wish to override.
     *
     * @throws SQLException if an error occurs while accessing the database
     */
    @Override
    public void apply(Connection c) throws SQLException {
        Statement statement = c.createStatement();
        String sep = this.sql.indexOf('\n') != -1 ? "\n" : " ";
        this.log.info("executing SQL statement:" + sep + this.sql);
        try {
            statement.execute(this.sql);
        } finally {
            statement.close();
        }
    }
}

