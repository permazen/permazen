
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.sql;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Support superclass for {@link KVImplementation}s that create {@link SQLKVDatabase} instances.
 */
public abstract class SQLDriverKVImplementation extends KVImplementation {

    private final String driverClassName;

    /**
     * Constructor.
     *
     * @param driverClassName {@link java.sql.Driver} implementation class name
     */
    protected SQLDriverKVImplementation(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    @Override
    public KVDatabase createKVDatabase(Object configuration, KVDatabase kvdb, AtomicKVStore kvstore) {

        // Load driver class
        try {
            Class.forName(this.driverClassName);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("can't load SQL driver class `" + this.driverClassName + "'", e);
        }

        // Extract JDBC URL from configuration
        final String jdbcUrl = this.getJdbcUrl(configuration);

        // Instantiate and configure KVDatabase
        final SQLKVDatabase sqlKV = this.createSQLKVDatabase(configuration);
        sqlKV.setDataSource(new DriverManagerDataSource(jdbcUrl));
        return sqlKV;
    }

    /**
     * Instantiate a {@link SQLKVDatabase}.
     *
     * <p>
     * This method does not need to configure the {@link javax.sql.DataSource} (via
     * {@link SQLKVDatabase#setDataSource SQLKVDatabase.setDataSource()}); the calling method will do that.
     *
     * @param configuration implementation configuration returned by {@link #parseCommandLineOptions parseCommandLineOptions()}
     * @return new key/value database
     */
    protected abstract SQLKVDatabase createSQLKVDatabase(Object configuration);

    /**
     * Extract the JDBC URL from the configuration object.
     *
     * <p>
     * The implementation in {@link SQLDriverKVImplementation} assumes {@code configuration}
     * is a {@link String}, and returns it.
     *
     * @param configuration implementation configuration returned by {@link #parseCommandLineOptions parseCommandLineOptions()}
     * @return JDBC driver URL
     */
    protected String getJdbcUrl(Object configuration) {
        return (String)configuration;
    }
}
