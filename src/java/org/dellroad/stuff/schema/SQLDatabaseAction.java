
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
 * A {@link DatabaseAction} that executes a configured SQL script.
 *
 * <p>
 * The script may contain multiple SQL statements, in which case individual statements will be parsed out and
 * executed individually in order. However, this requires proper configuration of the {@link #setSplitPattern split pattern}.
 *
 * <p>
 * When using Spring, beans of this type can be created succintly using the <code>&lt;dellroad-stuff:sql&gt;</code> custom
 * XML element. The split pattern may be configured via the {@code split-pattern} attribute, and the SQL script is specified
 * either directly via inline text or using the {@code resource} attribute. In the latter case, the character encoding can
 * specified via the {@code charset} attribute (default is {@code "UTF-8"}).
 *
 * <p>
 * For example:
 * <blockquote><pre>
 *  &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *    <b>xmlns:dellroad-stuff="http://dellroad-stuff.googlecode.com/schema/dellroad-stuff"</b>
 *    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *    xsi:schemaLocation="
 *      http://www.springframework.org/schema/beans
 *        http://www.springframework.org/schema/beans/spring-beans-3.0.xsd
 *      <b>http://dellroad-stuff.googlecode.com/schema/dellroad-stuff
 *        http://dellroad-stuff.googlecode.com/svn/wiki/schemas/dellroad-stuff-1.0.xsd</b>"&gt;
 *
 *      &lt;!-- SQL action that clears the audit log --&gt;
 *      <b>&lt;dellroad-stuff:sql&gt;DELETE * FROM AUDIT_LOG&lt;/dellroad-stuff:sql&gt;</b>
 *
 *      &lt;!-- A more complicated external SQL script --&gt;
 *      <b>&lt;dellroad-stuff:sql resource="classpath:reset.sql" split-pattern=";\n"/&gt;</b>
 *
 *      &lt;!-- other beans... --&gt;
 *
 *  &lt;/beans&gt;
 * </pre></blockquote>
 */
public class SQLDatabaseAction implements DatabaseAction {

    /**
     * The default split pattern: <code>{@value}</code>.
     */
    public static final String DEFAULT_SPLIT_PATTERN = ";[ \\t\\r]*\\n\\s*";

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private String sqlScript;
    private String splitPattern = DEFAULT_SPLIT_PATTERN;

    /**
     * Configure the SQL script. This is a required property.
     *
     * <p>
     * For scripts in external resources, consider {@link org.dellroad.stuff.spring.ResourceReaderFactoryBean}
     * or use the <code>&lt;dellroad-stuff:sql&gt;</code> element.
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
     *
     * <p>
     * The default value for this property is <code>";[ \t\r]*\n\s*"</code>, which should handle cases where
     * SQL statements are terminated by semi-colons and each SQL statement starts on a new line.
     *
     * <p>
     * If this is set to {@code null}, or the script does not contain any instances of the regular expression,
     * the script is assumed to contain a single SQL statement. SQL statements are whitespace-trimmed and any
     * "statements" that consist entirely of whitespace are ignored.
     *
     * @throws java.util.regex.PatternSyntaxException if the pattern is not a valid Java regular expression
     */
    public void setSplitPattern(String splitPattern) {
        if (splitPattern != null)
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

