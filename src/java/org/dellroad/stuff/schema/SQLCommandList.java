
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Holds and executes a configured SQL script, possibly containing multiple statements.
 *
 * <p>
 * If the script contains multiple SQL statements, individual statements will be executed individually, in order, by
 * {@link #apply apply()}; however, this requires proper configuration of the {@linkplain #setSplitPattern split pattern}.
 *
 * <p>
 * When using Spring, beans of this type can be created succintly using the <code>&lt;dellroad-stuff:sql&gt;</code> custom
 * XML element. The {@linkplain #setSplitPattern split pattern} may be configured via the {@code split-pattern} attribute,
 * and the SQL script is specified either directly via inline text or using the {@code resource} attribute. In the latter case,
 * the character encoding can specified via the {@code charset} attribute (default is {@code "UTF-8"}).
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
 *      &lt;!-- A more complicated, externally referenced, SQL script --&gt;
 *      <b>&lt;dellroad-stuff:sql resource="classpath:reset.sql" split-pattern=";\n"/&gt;</b>
 *
 *      &lt;!-- other beans... --&gt;
 *
 *  &lt;/beans&gt;
 * </pre></blockquote>
 */
public class SQLCommandList implements DatabaseAction<Connection> {

    /**
     * The default split pattern: <code>{@value}</code>.
     */
    public static final String DEFAULT_SPLIT_PATTERN = ";[ \\t\\r]*\\n\\s*";

    private String sqlScript;
    private String splitPattern = DEFAULT_SPLIT_PATTERN;

    public SQLCommandList() {
    }

    public SQLCommandList(String sqlScript) {
        this.setSQLScript(sqlScript);
    }

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

    /**
     * Applies each individual SQL command in the script. Commands are separated
     * using the {@link #setSplitPattern split pattern}.
     */
    @Override
    public void apply(Connection c) throws SQLException {
        for (SQLCommand sqlCommand : this.split())
            sqlCommand.apply(c);
    }

    /**
     * Split the SQL script into individual statements and return them as {@link DatabaseAction}s.
     */
    public List<SQLCommand> split() {
        ArrayList<SQLCommand> list = new ArrayList<SQLCommand>();
        for (String sql : this.splitSQL())
            list.add(new SQLCommand(sql));
        return list;
    }

    /**
     * Split the {@linkplain #setSQLScript configured SQL script} into individual SQL statements
     * using the configured {@linkplain #setSplitPattern split pattern}.
     *
     * @return an array of individual SQL statements
     * @throws IllegalArgumentException if no SQL script is configured
     */
    public String[] splitSQL() {
        if (this.sqlScript == null)
            throw new IllegalArgumentException("no SQL script configured");
        String[] sqls = this.splitPattern != null ? this.sqlScript.split(this.splitPattern) : new String[] { this.sqlScript };
        ArrayList<String> list = new ArrayList<String>(sqls.length);
        for (String sql : sqls) {
            sql = sql.trim();
            if (sql.length() == 0)
                continue;
            list.add(sql);
        }
        return list.toArray(new String[list.size()]);
    }
}

