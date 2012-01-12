
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;

import org.dellroad.stuff.spring.BeanNameComparator;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

/**
 * {@link SQLSchemaUpdater} optimized for use with Spring.
 * <ul>
 * <li>{@link #apply(Connection, DatabaseAction) apply()} is overridden so Spring {@link DataAccessException}s are thrown.</li>
 * <li>{@link #indicatesUninitializedDatabase indicatesUninitializedDatabase()} is overridden to examine exceptions
 *  and more precisely using Spring's exception translation infrastructure to filter out false positives.</li>
 * <li>{@link #getOrderingTieBreaker} is overridden to break ties by ordering updates in the same order
 *  as they are defined in the bean factory.</li>
 * <li>This class implements {@link InitializingBean} and verifies all required properties are set.</li>
 * <li>If no updates are {@linkplain #setUpdates explicitly configured}, then all {@link SpringSQLSchemaUpdate}s found
 *  in the containing bean factory are automatically configured.
 * </ul>
 *
 * <p>
 * An example of how this class can be combined with custom XML to define an updater, all its updates,
 * and a {@link SchemaUpdatingDataSource} that automatically updates the database schema:
 * <blockquote><pre>
 *  &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *    <b>xmlns:dellroad-stuff="http://dellroad-stuff.googlecode.com/schema/dellroad-stuff"</b>
 *    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *    xmlns:p="http://www.springframework.org/schema/p"
 *    xsi:schemaLocation="
 *      http://www.springframework.org/schema/beans
 *        http://www.springframework.org/schema/beans/spring-beans-3.0.xsd
 *      <b>http://dellroad-stuff.googlecode.com/schema/dellroad-stuff
 *        http://dellroad-stuff.googlecode.com/svn/wiki/schemas/dellroad-stuff-1.0.xsd</b>"&gt;
 *
 *     &lt;!-- DataSource that automatically updates the database schema --&gt;
 *     <b>&lt;bean id="dataSource" class="org.dellroad.stuff.schema.SchemaUpdatingDataSource"
 *       p:dataSource-ref="realDataSource" p:schemaUpdater-ref="schemaUpdater"/&gt;</b>
 *
 *     &lt;!--
 *          Database updater bean. This is used on first access to the DataSource above. Notes:
 *            - "databaseInitialization" is used to initialize the schema (first time only)
 *            - "updateTableInitialization" is used to initialize the update table (first time only)
 *            - In this example, we just use dellroad-stuff's update table initialization for MySQL
 *            - The &lt;dellroad-stuff:sql-update&gt; beans below will be auto-detected
 *     --&gt;
 *     <b>&lt;bean id="schemaUpdater" class="org.dellroad.stuff.schema.SpringSQLSchemaUpdater"&gt;
 *         &lt;property name="databaseInitialization"&gt;
 *             &lt;dellroad-stuff:sql resource="classpath:databaseInit.sql"/&gt;
 *         &lt;/property&gt;
 *         &lt;property name="updateTableInitialization"&gt;
 *             &lt;dellroad-stuff:sql resource="classpath:org/dellroad/stuff/schema/updateTable-mysql.sql"/&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;</b>
 *
 *      &lt;!-- Schema update to add the 'phone' column to the 'User' table --&gt;
 *      <b>&lt;dellroad-stuff:sql-update id="addPhone"&gt;ALTER TABLE User ADD phone VARCHAR(64)&lt;/dellroad-stuff:sql-update&gt;</b>
 *
 *      &lt;!-- Schema update to run some complicated external SQL script --&gt;
 *      <b>&lt;dellroad-stuff:sql-update id="majorChanges" depends-on="addPhone" resource="classpath:majorChanges.sql"/&gt;</b>
 *
 *      &lt;!-- Multiple SQL commands that will be automatically separated into distinct updates --&gt;
 *      <b>&lt;dellroad-stuff:sql-update id="renameColumn"&gt;
 *          ALTER TABLE User ADD newName VARCHAR(64);
 *          ALTER TABLE User SET newName = oldName;
 *          ALTER TABLE User DROP oldName;
 *      &lt;/dellroad-stuff:sql-update&gt;</b>
 *
 *      &lt;!-- Add more schema updates over time as needed and everything just works... --&gt;
 *
 *  &lt;/beans&gt;
 * </pre></blockquote>
 *
 * <p>
 * It is required that this updater and all of its schema updates are defined in the same {@link ListableBeanFactory}.
 */
public class SpringSQLSchemaUpdater extends SQLSchemaUpdater implements BeanFactoryAware, InitializingBean {

    private ListableBeanFactory beanFactory;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.beanFactory == null)
            throw new Exception("no bean factory configured");
        if (this.getDatabaseInitialization() == null)
            throw new Exception("no database initialization configured");
        if (this.getUpdateTableInitialization() == null)
            throw new Exception("no update table initialization configured");
        if (this.getUpdates() == null) {
            this.setUpdates(new ArrayList<SchemaUpdate<Connection>>(
              this.beanFactory.getBeansOfType(SpringSQLSchemaUpdate.class).values()));
        }
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        if (!(beanFactory instanceof ListableBeanFactory))
            throw new IllegalArgumentException("containing BeanFactory is not a ListableBeanFactory: " + beanFactory);
        this.beanFactory = (ListableBeanFactory)beanFactory;
    }


    /**
     * Determine if an exception thrown during {@link #databaseNeedsInitialization} is consistent with
     * an uninitialized database.
     *
     * <p>
     * The implementation in {@link SpringSQLSchemaUpdater} looks for a {@link BadSqlGrammarException}.
     */
    @Override
    protected boolean indicatesUninitializedDatabase(Connection c, SQLException e) throws SQLException {
        return this.translate(e, c, null) instanceof BadSqlGrammarException;
    }

    /**
     * Apply a {@link DatabaseAction} to a {@link Connection}.
     *
     * <p>
     * The implementation in {@link SQLSchemaUpdater} invokes the action and delegates to
     * {@link #translate(SQLException, Connection, String) translate()} to convert any {@link SQLException} thrown.
     *
     * @throws SQLException if an error occurs attempting to translate a thrown SQLException
     * @throws DataAccessException if an error occurs accessing the database
     * @see #translate(SQLException, Connection, String) translate()
     */
    @Override
    protected void apply(Connection c, DatabaseAction<Connection> action) throws SQLException {
        try {
            super.apply(c, action);
        } catch (SQLException e) {
            String sql = action instanceof SQLCommand ? ((SQLCommand)action).getSQL() : null;
            throw this.translate(e, c, sql);
        }
    }

    /**
     * Converts {@link SQLException}s into Spring {@link DataAccessException}s.
     */
    protected DataAccessException translate(SQLException e, Connection c, String sql) throws SQLException {
        return new SQLErrorCodeSQLExceptionTranslator(c.getMetaData().getDatabaseProductName())
          .translate("database access during schema update", sql, e);
    }

    /**
     * Get the preferred ordering of two updates that do not have any predecessor constraints
     * (including implied indirect constraints) between them.
     *
     * <p>
     * The {@link Comparator} returned by the implementation in {@link SpringSQLSchemaUpdater} sorts
     * updates in the same order that they appear in the {@link BeanFactory}.
     */
    @Override
    protected Comparator<SchemaUpdate<Connection>> getOrderingTieBreaker() {
        final BeanNameComparator beanNameComparator = new BeanNameComparator(this.beanFactory);
        return new Comparator<SchemaUpdate<Connection>>() {
            @Override
            public int compare(SchemaUpdate<Connection> update1, SchemaUpdate<Connection> update2) {
                return beanNameComparator.compare(update1.getName(), update2.getName());
            }
        };
    }
}

