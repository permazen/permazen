
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
 *
 * @param <C> configuration object type
 */
public abstract class SQLDriverKVImplementation<C extends SQLDriverKVImplementation.Config> extends KVImplementation<C> {

    private final String driverClassName;

    /**
     * Constructor.
     *
     * @param configType configuration object type
     * @param driverClassName {@link java.sql.Driver} implementation class name
     */
    protected SQLDriverKVImplementation(Class<C> configType, String driverClassName) {
        super(configType);
        this.driverClassName = driverClassName;
    }

    @Override
    public KVDatabase createKVDatabase(C config, KVDatabase kvdb, AtomicKVStore kvstore) {

        // Load driver class
        try {
            Class.forName(this.driverClassName, false, Thread.currentThread().getContextClassLoader());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("can't load SQL driver class \"" + this.driverClassName + "\"", e);
        }

        // Extract JDBC URL from configuration
        final String jdbcUrl = config.getJdbcUrl();

        // Instantiate and configure KVDatabase
        final SQLKVDatabase sqlKV = this.createSQLKVDatabase(config);
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
     * @param config implementation configuration returned by {@link #parseCommandLineOptions parseCommandLineOptions()}
     * @return new key/value database
     */
    protected abstract SQLKVDatabase createSQLKVDatabase(C config);

// Options

    public static class Config {

        private String url;

        public Config() {
        }

        public Config(String url) {
            this.setJdbcUrl(url);
        }

        public String getJdbcUrl() {
            return this.url;
        }
        public void setJdbcUrl(String url) {
            this.url = url;
        }
    }
}
