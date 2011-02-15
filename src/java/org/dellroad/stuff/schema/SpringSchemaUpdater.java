
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * {@link SchemaUpdater} optimized for use with Spring.
 * <ul>
 * <li>{@link #applyAction(DataSource, DatabaseAction)} is overridden to use Spring's {@link JdbcTemplate}
 *  so Spring {@link org.springframework.dao.DataAccessException}s are thrown.</li>
 * <li>{@link #databaseNeedsInitialization databaseNeedsInitialization()} is overridden to catch exceptions
 *  more precisely to filter out false positives.</li>
 * <li>{@link #getOrderingTieBreaker} is overridden to break ties by ordering updates in the same order
 *  as they are defined in the bean factory.</li>
 * <li>This class implements {@link InitializingBean} and verifies all required properties are set.</li>
 * <li>If no updates are {@link #setUpdates explicitly configured}, then all {@link SchemaUpdate}s found
 *  in the containing bean factory are automatically configured.
 * </ul>
 *
 * <p>
 * It is required that this updater and all of its schema updates are defined in the same {@link ListableBeanFactory}.
 */
public class SpringSchemaUpdater extends SchemaUpdater implements BeanFactoryAware, InitializingBean {

    private ListableBeanFactory beanFactory;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.beanFactory == null)
            throw new Exception("no bean factory configured");
        if (this.getDatabaseInitialization() == null)
            throw new Exception("no database initialization configured");
        if (this.getUpdateTableInitialization() == null)
            throw new Exception("no update table initialization configured");
        if (this.getUpdates() == null)
            this.setUpdates(this.beanFactory.getBeansOfType(SchemaUpdate.class).values());
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        if (!(beanFactory instanceof ListableBeanFactory))
            throw new IllegalArgumentException("containing BeanFactory is not a ListableBeanFactory: " + beanFactory);
        this.beanFactory = (ListableBeanFactory)beanFactory;
    }

    /**
     * Determine if the database needs initialization.
     *
     * <p>
     * The implementation in {@link SpringSchemaUpdater} invokes <code>SELECT COUNT(*) FROM <i>UPDATETABLE</i></code>
     * and checks for a {@link BadSqlGrammarException}.
     */
    @Override
    public boolean databaseNeedsInitialization(DataSource dataSource) throws SQLException {
        try {
            long numUpdates = new JdbcTemplate(dataSource).queryForLong("SELECT COUNT(*) FROM " + this.getUpdateTableName());
            this.log.info("detected already initialized database, with " + numUpdates + " update(s) already applied");
            return false;
        } catch (BadSqlGrammarException e) {
            log.warn("detected uninitialized database: update table `" + this.getUpdateTableName() + "' not found: " + e);
            return true;
        }
    }

    /**
     * Apply a {@link DatabaseAction} to a {@link DataSource}.
     *
     * <p>
     * The implementation in {@link SpringSchemaUpdater} uses {@link JdbcTemplate} to apply the modification so that
     * Spring {@link org.springframework.dao.DataAccessException}s are thrown in case of errors.
     */
    @Override
    protected void applyAction(DataSource dataSource, final DatabaseAction action) throws SQLException {
        new JdbcTemplate(dataSource).execute(new ConnectionCallback<Void>() {
            @Override
            public Void doInConnection(Connection c) throws SQLException {
                action.apply(c);
                return null;
            }
        });
    }

    /**
     * Get the preferred ordering of two updates that do not have any predecessor constraints
     * (including implied indirect constraints) between them.
     *
     * <p>
     * The {@link Comparator} returned by the implementation in {@link SpringSchemaUpdater} sorts
     * updates in the same order that they appear in the {@link BeanFactory}.
     */
    @Override
    protected Comparator<SchemaUpdate> getOrderingTieBreaker() {
        String[] beanNames = this.beanFactory.getBeanDefinitionNames();
        final Map<String, Integer> sort = new HashMap<String, Integer>(beanNames.length);
        for (int i = 0; i < beanNames.length; i++)
            sort.put(beanNames[i], i);
        return new Comparator<SchemaUpdate>() {
            @Override
            public int compare(SchemaUpdate update1, SchemaUpdate update2) {
                String[] names = new String[] { update1.getName(), update2.getName() };
                Integer[] indexes = new Integer[] { sort.get(update1.getName()), sort.get(update2.getName()) };
                for (int i = 0; i < 2; i++) {
                    if (indexes[i] == null) {
                        throw new IllegalArgumentException("failed to find update `" + names[i]
                          + "' in bean factory " + SpringSchemaUpdater.this.beanFactory);
                    }
                }
                return indexes[0] - indexes[1];
            };
        };
    }
}

