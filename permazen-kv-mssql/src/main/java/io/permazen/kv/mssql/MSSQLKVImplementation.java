
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mssql;

import io.permazen.kv.sql.SQLDriverKVImplementation;

import java.net.URI;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class MSSQLKVImplementation extends SQLDriverKVImplementation<SQLDriverKVImplementation.Config> {

    public static final String MSSQL_DRIVER_CLASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    /**
     * Default constructor.
     */
    public MSSQLKVImplementation() {
        this(MSSQL_DRIVER_CLASS_NAME);
    }

    /**
     * Constructor allowing alternative driver.
     *
     * @param driverClassName SQL Server {@link java.sql.Driver} implementation class name
     */
    public MSSQLKVImplementation(String driverClassName) {
        super(driverClassName);
    }

    @Override
    protected void addJdbcUriOption(OptionParser parser) {
        this.addJdbcUriOption(parser, "mssql", "Use MS SQL Server key/value store with the given JDBC URL");
    }

    @Override
    protected Config buildConfig(OptionSet options, URI uri) {
        return new Config(uri);
    }

    @Override
    public String getDescription(Config configuration) {
        return "SQL Server";
    }

    @Override
    protected MSSQLKVDatabase createSQLKVDatabase(Config configuration) {
        return new MSSQLKVDatabase();
    }
}
