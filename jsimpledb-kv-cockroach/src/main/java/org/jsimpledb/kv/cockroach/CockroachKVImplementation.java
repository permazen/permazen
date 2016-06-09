
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.cockroach;

import java.util.ArrayDeque;

import org.jsimpledb.kv.sql.SQLDriverKVImplementation;

public class CockroachKVImplementation extends SQLDriverKVImplementation {

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
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--cockroach URL", "Use CockroachDB key/value store with the given JDBC URL" },
        };
    }

    @Override
    public String parseCommandLineOptions(ArrayDeque<String> options) {
        return this.parseCommandLineOption(options, "--cockroach");
    }

    @Override
    public String getDescription(Object configuration) {
        return "CockroachDB";
    }

    @Override
    protected CockroachKVDatabase createSQLKVDatabase(Object configuration) {
        return new CockroachKVDatabase();
    }
}
