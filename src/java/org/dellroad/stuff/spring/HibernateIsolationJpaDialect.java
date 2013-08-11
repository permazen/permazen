
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import java.sql.Connection;
import java.sql.SQLException;

import javax.persistence.EntityManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.UncategorizedDataAccessException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.orm.jpa.vendor.HibernateJpaDialect;
import org.springframework.transaction.TransactionDefinition;

/**
 * Hibernate JPA adapter that adds supports for setting a non-default transaction isolation level
 * and adds a workaround for <a href="https://jira.springsource.org/browse/SPR-10815">SPR-10815</a>.
 *
 * <p>
 * The JPA specification, which is database technology agnostic, does not define a way to set SQL database isolation levels.
 * This fixes the "Standard JPA does not support custom isolation levels - use a special JpaDialect" Spring exception
 * that will otherwise occur.
 *
 * @see <a href="https://jira.springsource.org/browse/SPR-3812">SPR-3812</a>
 * @see <a href="http://stackoverflow.com/questions/5234240/hibernatespringjpaisolation-does-not-work">Question on stackoverflow.com</a>
 * @see <a href="https://jira.springsource.org/browse/SPR-10815">SPR-10815</a>
 */
@SuppressWarnings("serial")
public class HibernateIsolationJpaDialect extends HibernateJpaDialect {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public TransactionDataWrapper beginTransaction(EntityManager entityManager, TransactionDefinition definition)
      throws SQLException {

        // Set transaction timeout
        if (definition.getTimeout() != TransactionDefinition.TIMEOUT_DEFAULT)
            this.getSession(entityManager).getTransaction().setTimeout(definition.getTimeout());

        // Set isolation level on the associated JDBC connection
        Integer oldIsolation = null;
        Connection connection = null;
        if (definition.getIsolationLevel() != TransactionDefinition.ISOLATION_DEFAULT) {
            connection = this.getJdbcConnection(entityManager, definition.isReadOnly()).getConnection();
            if (this.log.isTraceEnabled())
                this.log.trace("setting isolation level to " + definition.getIsolationLevel() + " on " + connection);
            oldIsolation = DataSourceUtils.prepareConnectionForTransaction(connection, definition);
        }

        // Start transaction
        entityManager.getTransaction().begin();

        // Prepare transaction
        return this.prepareTransaction(entityManager, definition, connection, oldIsolation);
    }

    /**
     * Prepare transaction.
     */
    protected TransactionDataWrapper prepareTransaction(EntityManager entityManager, TransactionDefinition definition,
      Connection connection, Integer oldIsolation) {

        // Do superclass preparation
        Object transactionData = this.prepareTransaction(entityManager, definition.isReadOnly(), definition.getName());

        // Wrap result
        return new TransactionDataWrapper(transactionData, connection, oldIsolation);
    }

    @Override
    public void cleanupTransaction(Object obj) {

        // Let superclass cleanup
        final TransactionDataWrapper wrapper = (TransactionDataWrapper)obj;
        super.cleanupTransaction(wrapper.getTransactionData());

        // Restore isolation level on the JDBC connection
        Connection connection = wrapper.getConnection();
        if (connection != null) {
            if (this.log.isTraceEnabled())
                this.log.trace("restoring isolation level to " + wrapper.getIsolation() + " on " + connection);
            DataSourceUtils.resetConnectionAfterTransaction(connection, wrapper.getIsolation());
        }
    }

    /**
     * Adds fix for SPR-10815.
     *
     * @see <a href="https://jira.springsource.org/browse/SPR-10815">SPR-10815</a>
     */
    @Override
    public DataAccessException translateExceptionIfPossible(RuntimeException ex) {

        // Handle case that Spring has already wrapped and hidden the real exception without recognizing it
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof RuntimeException) {
                final DataAccessException e = super.translateExceptionIfPossible((RuntimeException)t);
                if (e != null && !(e instanceof UncategorizedDataAccessException))
                    return e;
            }
        }

        // Give up
        return super.translateExceptionIfPossible(ex);
    }

    /**
     * Wraps superclass transaction data and adds isolation level to restore.
     */
    protected class TransactionDataWrapper {

        private final Object transactionData;
        private final Connection connection;
        private final Integer isolation;

        public TransactionDataWrapper(Object transactionData, Connection connection, Integer isolation) {
            this.transactionData = transactionData;
            this.connection = connection;
            this.isolation = isolation;
        }

        public Object getTransactionData() {
            return this.transactionData;
        }

        public Connection getConnection() {
            return this.connection;
        }

        public Integer getIsolation() {
            return this.isolation;
        }
    }
}

