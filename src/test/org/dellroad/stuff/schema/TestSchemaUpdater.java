
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

public class TestSchemaUpdater extends SpringSchemaUpdater {

    public static final String PLACEHOLDER_SQL = "sql-statement-placeholder";

    private boolean databaseNeedsInitialization;

    private final TestDatabaseAction databaseInitialization = new TestDatabaseAction();
    private final TestDatabaseAction updateTableInitialization = new TestDatabaseAction();
    private final ArrayList<String> updatesRecorded = new ArrayList<String>();

    private Set<String> previousUpdates;

    @Override
    public DatabaseAction getDatabaseInitialization() {
        return this.databaseInitialization;
    }

    @Override
    public DatabaseAction getUpdateTableInitialization() {
        return this.updateTableInitialization;
    }

    public void checkInitialization() {
        int expectedCount = this.databaseNeedsInitialization ? 1 : 0;
        this.databaseInitialization.checkCount(expectedCount);
        this.updateTableInitialization.checkCount(expectedCount);
    }

    @Override
    public boolean databaseNeedsInitialization(DataSource dataSource) {
        return this.databaseNeedsInitialization;
    }
    public void setDatabaseNeedsInitialization(boolean databaseNeedsInitialization) {
        this.databaseNeedsInitialization = databaseNeedsInitialization;
    }

    @Override
    protected void recordUpdateApplied(Connection c, String name) throws SQLException {
        this.updatesRecorded.add(name);
    }

    @Override
    protected Set<String> getAppliedUpdateNames(Connection c) throws SQLException {
        return this.previousUpdates;
    }

    public List<String> getUpdatesRecorded() {
        return this.updatesRecorded;
    }

    public void setPreviousUpdates(Set<String> previousUpdates) {
        this.previousUpdates = previousUpdates;
    }
}

