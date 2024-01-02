
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.sql;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;

import java.net.URI;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Support superclass for {@link KVImplementation}s that create {@link SQLKVDatabase} instances.
 *
 * @param <C> configuration object type
 */
public abstract class SQLDriverKVImplementation<C extends SQLDriverKVImplementation.Config> implements KVImplementation<C> {

    protected OptionSpec<URI> jdbcUriOption;

    private final String driverClassName;

    /**
     * Constructor.
     *
     * @param driverClassName {@link java.sql.Driver} implementation class name
     */
    protected SQLDriverKVImplementation(String driverClassName) {
        Preconditions.checkArgument(driverClassName != null, "null driverClassName");
        this.driverClassName = driverClassName;
    }

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        this.addJdbcUriOption(parser);
    }

    protected abstract void addJdbcUriOption(OptionParser parser);

    protected void addJdbcUriOption(OptionParser parser, String option, String description) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.jdbcUriOption == null, "duplicate option");
        this.jdbcUriOption = parser.accepts(option, description)
          .withRequiredArg()
          .describedAs("url")
          .ofType(URI.class);
    }

    @Override
    public C buildConfig(OptionSet options) {
        final URI uri = this.jdbcUriOption.value(options);
        if (uri == null)
            return null;
        return this.buildConfig(options, uri);
    }

    protected abstract C buildConfig(OptionSet options, URI uri);

    @Override
    public KVDatabase createKVDatabase(C config, KVDatabase kvdb, AtomicKVStore kvstore) {

        // Load driver class
        try {
            Class.forName(this.driverClassName, false, Thread.currentThread().getContextClassLoader());
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("can't load SQL driver class \"%s\"", this.driverClassName), e);
        }

        // Extract JDBC URL from configuration
        final URI jdbcUri = config.getJdbcUri();

        // Instantiate and configure KVDatabase
        final SQLKVDatabase sqlKV = this.createSQLKVDatabase(config);
        sqlKV.setDataSource(new DriverManagerDataSource(jdbcUri.toString()));
        return sqlKV;
    }

    /**
     * Instantiate a {@link SQLKVDatabase}.
     *
     * <p>
     * This method does not need to configure the {@link javax.sql.DataSource} (via
     * {@link SQLKVDatabase#setDataSource SQLKVDatabase.setDataSource()}); the calling method will do that.
     *
     * @param config implementation configuration returned by {@link #buildConfig buildConfig()}
     * @return new key/value database
     */
    protected abstract SQLKVDatabase createSQLKVDatabase(C config);

// Options

    public static class Config {

        private URI uri;

        public Config() {
        }

        public Config(URI uri) {
            this.setJdbcUri(uri);
        }

        public URI getJdbcUri() {
            return this.uri;
        }
        public void setJdbcUri(URI uri) {
            this.uri = uri;
        }
    }
}
