
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.cockroach;

import io.permazen.kv.sql.SQLDriverKVImplementation;

import java.net.URI;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class CockroachKVImplementation extends SQLDriverKVImplementation<SQLDriverKVImplementation.Config> {

    public static final String POSTGRESQL_DRIVER_CLASS_NAME = "org.postgresql.Driver";

    /**
     * Default constructor.
     */
    public CockroachKVImplementation() {
        this(POSTGRESQL_DRIVER_CLASS_NAME);
    }

    /**
     * Constructor allowing alternative driver.
     *
     * @param driverClassName Cockroach {@link java.sql.Driver} implementation class name
     */
    public CockroachKVImplementation(String driverClassName) {
        super(driverClassName);
    }

    @Override
    protected void addJdbcUriOption(OptionParser parser) {
        this.addJdbcUriOption(parser, "cockroach", "Use CockroachDB key/value store with the given JDBC URL");
    }

    @Override
    protected Config buildConfig(OptionSet options, URI uri) {
        return new Config(uri);
    }

    @Override
    public String getDescription(Config configuration) {
        return "CockroachDB";
    }

    @Override
    protected CockroachKVDatabase createSQLKVDatabase(Config config) {
        return new CockroachKVDatabase();
    }
}
