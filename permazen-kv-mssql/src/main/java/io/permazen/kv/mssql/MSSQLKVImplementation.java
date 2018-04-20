
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mssql;

import io.permazen.kv.sql.SQLDriverKVImplementation;

import java.util.ArrayDeque;

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
        super(Config.class, driverClassName);
    }

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--mssql URL", "Use SQL Server key/value store with the given JDBC URL" },
        };
    }

    @Override
    public Config parseCommandLineOptions(ArrayDeque<String> options) {
        final String url = this.parseCommandLineOption(options, "--mssql");
        return url != null ? new Config(url) : null;
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
