
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

/**
 * {@link Session} modes.
 *
 * <p>
 * The three {@link SessionMode} values correspond to the three layers of abstraction in JSimpleDB.
 * In order from bottom to top these are:
 * <ul>
 *  <li>The key/value database layer, providing {@link org.jsimpledb.kv.KVDatabase}
 *      and {@link org.jsimpledb.kv.KVTransaction} and represented by {@link #KEY_VALUE}</li>
 *  <li>The core API database layer, providing {@link org.jsimpledb.core.Database}
 *      and {@link org.jsimpledb.core.Transaction} and represented by {@link #CORE_API}</li>
 *  <li>The JSimpleDB (or "Java") layer, providing {@link org.jsimpledb.JSimpleDB}
 *      and {@link org.jsimpledb.JTransaction} and represented by {@link #JSIMPLEDB}</li>
 * </ul>
 */
public enum SessionMode {

    /**
     * Key/value database mode.
     *
     * <p>
     * This is the lowest level CLI mode, where only the raw {@code byte[]} key/value store is available.
     * This mode provides a {@link org.jsimpledb.kv.KVTransaction}, but no {@link org.jsimpledb.core.Transaction}
     * or {@link org.jsimpledb.JTransaction}.
     *
     * @see org.jsimpledb.kv
     */
    KEY_VALUE(false, false),

    /**
     * Core API mode.
     *
     * <p>
     * Supports access to low level schema information, core API "objects" and fields, but no Java-specific operations.
     * This mode provides a {@link org.jsimpledb.kv.KVTransaction} and {@link org.jsimpledb.core.Transaction},
     * but no {@link org.jsimpledb.JTransaction}.
     *
     * <p>
     * Schema information may be provided either by a schema XML file or by
     * {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}-annotated Java model classes; however, in the latter
     * case these classes will be otherwise ignored.
     *
     * <p>
     * In this mode, database objects are represented by {@link org.jsimpledb.core.ObjId}'s and enum values by
     * {@link org.jsimpledb.core.EnumValue}'s.
     */
    CORE_API(false, true),

    /**
     * JSimpleDB (or "Java") mode.
     *
     * <p>
     * Provides
     * i.e., when the there are no {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}-annotated Java model
     * classes defined and the core {@link org.jsimpledb.core.Database} API is used.
     *
     * <p>
     * In this mode, database objects are represented by Java model class instances and enum values by corresponding
     * Java model {@link Enum} class instances.
     */
    JSIMPLEDB(true, true);

    private final boolean hasJSimpleDB;
    private final boolean hasCoreAPI;

    private SessionMode(boolean hasJSimpleDB, boolean hasCoreAPI) {
        this.hasJSimpleDB = hasJSimpleDB;
        this.hasCoreAPI = hasCoreAPI;
    }

    /**
     * Determine whether the JSimpleDB (or "Java") API (e.g., {@link org.jsimpledb.JTransaction}) is available in this mode.
     *
     * @return true if the JSimpleDB "Java" API is available
     */
    public boolean hasJSimpleDB() {
        return this.hasJSimpleDB;
    }

    /**
     * Determine whether the core API (e.g., {@link org.jsimpledb.core.Transaction}) is available in this mode.
     *
     * @return true if the core API is available
     */
    public boolean hasCoreAPI() {
        return this.hasCoreAPI;
    }
}

