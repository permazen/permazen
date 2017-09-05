
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mssql;

import java.util.ArrayDeque;

import io.permazen.kv.sql.SQLDriverKVImplementation;

public class MSSQLKVImplementation extends SQLDriverKVImplementation {

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
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--mssql URL", "Use SQL Server key/value store with the given JDBC URL" },
        };
    }

    @Override
    public String parseCommandLineOptions(ArrayDeque<String> options) {
        return this.parseCommandLineOption(options, "--mssql");
    }

    @Override
    public String getDescription(Object configuration) {
        return "SQL Server";
    }

    @Override
    protected MSSQLKVDatabase createSQLKVDatabase(Object configuration) {
        return new MSSQLKVDatabase();
    }
}
