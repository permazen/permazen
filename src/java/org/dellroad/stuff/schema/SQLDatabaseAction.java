
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DatabaseAction} that executes a provided SQL script.
 *
 * <p>
 * The script may contain multiple SQL statements, in which case individual statements will be parsed out
 * and executed individually in order. However, this requires proper configuration of a {@link #setSplitPattern split pattern}.
 */
public class SQLDatabaseAction implements DatabaseAction {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private String sqlScript;
    private String splitPattern;

    /**
     * Configure the SQL script. This is a required property.
     *
     * <p>
     * For scripts in external resources, consider {@link org.dellroad.stuff.spring.ResourceReaderFactoryBean}.
     *
     * @param sqlScript script containing one or more SQL statements; if more than one statement is present,
     *  a {@link #setSplitPattern split pattern} must also be configured
     * @see #setSplitPattern setSplitPattern()
     */
    public void setSQLScript(String sqlScript) {
        this.sqlScript = sqlScript;
    }

    /**
     * Set the <i>split pattern</i> used to split apart a script containing multiple SQL statements into individual statements.
     * If this property is not set, the script is assumed to contain a single SQL statement.
     *
     * <p>
     * For example, assuming statements are terminated by semi-colons and each SQL statement starts on a new line,
     * a reasonable setting would be <code>";[ \t\r]*\n\s*"</code>.
     *
     * @throws java.util.regex.PatternSyntaxException if the pattern is not a valid Java regular expression
     */
    public void setSplitPattern(String splitPattern) {
        Pattern.compile(splitPattern);
        this.splitPattern = splitPattern;
    }

    @Override
    public void apply(Connection c) throws SQLException {
        if (this.sqlScript == null)
            throw new IllegalArgumentException("no SQL script configured");
        String[] statements = this.splitPattern != null ? this.sqlScript.split(this.splitPattern) : new String[] { this.sqlScript };
        for (int i = 0; i < statements.length; i++) {
            String sql = statements[i].trim();
            if (sql.length() == 0)
                continue;
            String sep = sql.indexOf('\n') != -1 ? "\n" : " ";
            this.log.info("executing SQL statement:" + sep + sql);
            Statement statement = c.createStatement();
            try {
                statement.execute(sql);
            } catch (SQLException e) {
                this.log.error("SQL statement failed: " + sql, e);
                throw e;
            } finally {
                statement.close();
            }
        }
    }
}

