
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.cockroach;

import io.permazen.kv.sql.SQLDriverKVImplementation;

import java.util.ArrayDeque;

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
        super(Config.class, driverClassName);
    }

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--cockroach URL", "Use CockroachDB key/value store with the given JDBC URL" },
        };
    }

    @Override
    public Config parseCommandLineOptions(ArrayDeque<String> options) {
        return new Config(this.parseCommandLineOption(options, "--cockroach"));
    }

    @Override
    public String getDescription(Config configuration) {
        return "CockroachDB";
    }

    @Override
    protected CockroachKVDatabase createSQLKVDatabase(Config configuration) {
        return new CockroachKVDatabase();
    }
}
