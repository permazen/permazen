
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mysql;

import io.permazen.kv.sql.SQLDriverKVImplementation;

import java.net.URI;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class MySQLKVImplementation extends SQLDriverKVImplementation<SQLDriverKVImplementation.Config> {

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
    protected void addJdbcUriOption(OptionParser parser) {
        this.addJdbcUriOption(parser, "mysql", "Use MySQL key/value store with the given JDBC URL");
    }

    @Override
    protected Config buildConfig(OptionSet options, URI uri) {
        return new Config(uri);
    }

    @Override
    public String getDescription(Config configuration) {
        return "MySQL";
    }

    @Override
    protected MySQLKVDatabase createSQLKVDatabase(Config config) {
        return new MySQLKVDatabase();
    }
}
