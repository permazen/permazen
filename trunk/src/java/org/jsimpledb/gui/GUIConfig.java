
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import org.jsimpledb.JSimpleDB;

/**
 * GUI configuration object.
 */
public interface GUIConfig {

    /**
     * Get the {@link JSimpleDB}.
     */
    JSimpleDB getJSimpleDB();

    /**
     * Get a short description of the database.
     */
    String getDatabaseDescription();

    /**
     * Determine the schema version associated with the {@link JSimpleDB}.
     */
    int getSchemaVersion();

    /**
     * Determine whether we are allowed to create a new schema version.
     */
    boolean isAllowNewSchema();

    /**
     * Determine whether the underlying database is read-only.
     */
    boolean isReadOnly();

    /**
     * Determine whether verbose mode is enabled.
     */
    boolean isVerbose();

    /**
     * Get any custom {@link org.jsimpledb.parse.func.AbstractFunction} classes.
     */
    Iterable<Class<?>> getFunctionClasses();
}

