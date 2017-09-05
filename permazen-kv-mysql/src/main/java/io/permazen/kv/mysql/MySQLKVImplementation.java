
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mysql;

import io.permazen.kv.sql.SQLDriverKVImplementation;

import java.util.ArrayDeque;

public class MySQLKVImplementation extends SQLDriverKVImplementation {

    public static final String MYSQL_DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";

    /**
     * Default constructor.
     */
    public MySQLKVImplementation() {
        this(MYSQL_DRIVER_CLASS_NAME);
    }

    /**
     * Constructor allowing alternative driver.
     *
     * @param driverClassName MySQL {@link java.sql.Driver} implementation class name
     */
    public MySQLKVImplementation(String driverClassName) {
        super(driverClassName);
    }

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--mysql URL", "Use MySQL key/value store with the given JDBC URL" },
        };
    }

    @Override
    public String parseCommandLineOptions(ArrayDeque<String> options) {
        return this.parseCommandLineOption(options, "--mysql");
    }

    @Override
    public String getDescription(Object configuration) {
        return "MySQL";
    }

    @Override
    protected MySQLKVDatabase createSQLKVDatabase(Object configuration) {
        return new MySQLKVDatabase();
    }
}
