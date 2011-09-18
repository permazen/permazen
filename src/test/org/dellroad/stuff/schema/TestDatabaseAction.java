
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;

import org.testng.Assert;

public class TestDatabaseAction implements DatabaseAction {

    public static final SQLException TEST_EXCEPTION = new SQLException("test exception");

    private final boolean fail;
    private int count;

    public TestDatabaseAction() {
        this(false);
    }

    public TestDatabaseAction(boolean fail) {
        this.fail = fail;
    }

    @Override
    public final void apply(Connection c) throws SQLException {
        if (this.fail)
            throw TEST_EXCEPTION;
        this.count++;
    }

    public int getCount() {
        return this.count;
    }

    public void checkCount(int count) {
        Assert.assertEquals(count, this.count);
    }
}

