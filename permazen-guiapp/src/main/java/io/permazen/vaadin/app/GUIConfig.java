
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin.app;

import io.permazen.Permazen;

/**
 * Configuration object for the Permazen GUI application.
 */
public interface GUIConfig {

    /**
     * Get the {@link Permazen}.
     *
     * @return configured database
     */
    Permazen getPermazen();

    /**
     * Get a short description of the database.
     *
     * @return configured database description
     */
    String getDatabaseDescription();

    /**
     * Determine the schema version associated with the {@link Permazen}.
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
}

