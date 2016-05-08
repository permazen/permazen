
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.vaadin.app;

import org.jsimpledb.JSimpleDB;

/**
 * Configuration object for the JSimpleDB GUI application.
 */
public interface GUIConfig {

    /**
     * Get the {@link JSimpleDB}.
     *
     * @return configured database
     */
    JSimpleDB getJSimpleDB();

    /**
     * Get a short description of the database.
     *
     * @return configured database description
     */
    String getDatabaseDescription();

    /**
     * Determine the schema version associated with the {@link JSimpleDB}.
     *
     * @return configured schema version
     */
    int getSchemaVersion();

    /**
     * Determine whether we are allowed to create a new schema version.
     *
     * @return configured new schema setting
     */
    boolean isAllowNewSchema();

    /**
     * Determine whether the underlying database is read-only.
     *
     * @return configured read-only setting
     */
    boolean isReadOnly();

    /**
     * Determine whether verbose mode is enabled.
     *
     * @return configured verbose setting
     */
    boolean isVerbose();

    /**
     * Get any custom {@link org.jsimpledb.parse.func.AbstractFunction} classes.
     *
     * @return custom function classes, or null for none
     */
    Iterable<? extends Class<?>> getFunctionClasses();
}

