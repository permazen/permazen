
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

/**
 * {@link Session} modes.
 *
 * <p>
 * The three {@link SessionMode} values correspond to the three layers of abstraction in JSimpleDB.
 * In order from bottom to top these are:
 * <ul>
 *  <li>The key/value database layer, providing {@link io.permazen.kv.KVDatabase}
 *      and {@link io.permazen.kv.KVTransaction} and represented by {@link #KEY_VALUE}</li>
 *  <li>The core API database layer, providing {@link io.permazen.core.Database}
 *      and {@link io.permazen.core.Transaction} and represented by {@link #CORE_API}</li>
 *  <li>The JSimpleDB (or "Java") layer, providing {@link io.permazen.JSimpleDB}
 *      and {@link io.permazen.JTransaction} and represented by {@link #JSIMPLEDB}</li>
 * </ul>
 */
public enum SessionMode {

    /**
     * Key/value database mode.
     *
     * <p>
     * This is the lowest level CLI mode, where only the raw {@code byte[]} key/value store is available.
     * This mode provides a {@link io.permazen.kv.KVTransaction}, but no {@link io.permazen.core.Transaction}
     * or {@link io.permazen.JTransaction}.
     *
     * @see io.permazen.kv
     */
    KEY_VALUE(false, false),

    /**
     * Core API mode.
     *
     * <p>
     * Supports access to low level schema information, core API "objects" and fields, but no Java-specific operations.
     * This mode provides a {@link io.permazen.kv.KVTransaction} and {@link io.permazen.core.Transaction},
     * but no {@link io.permazen.JTransaction}.
     *
     * <p>
     * Schema information may be provided either by a schema XML file or by
     * {@link io.permazen.annotation.PermazenType &#64;PermazenType}-annotated Java model classes; however, in the latter
     * case these classes will be otherwise ignored.
     *
     * <p>
     * In this mode, database objects are represented by {@link io.permazen.core.ObjId}'s and enum values by
     * {@link io.permazen.core.EnumValue}'s.
     */
    CORE_API(false, true),

    /**
     * JSimpleDB (or "Java") mode.
     *
     * <p>
     * Provides
     * i.e., when the there are no {@link io.permazen.annotation.PermazenType &#64;PermazenType}-annotated Java model
     * classes defined and the core {@link io.permazen.core.Database} API is used.
     *
     * <p>
     * In this mode, database objects are represented by Java model class instances and enum values by corresponding
     * Java model {@link Enum} class instances.
     */
    JSIMPLEDB(true, true);

    private final boolean hasJSimpleDB;
    private final boolean hasCoreAPI;

    SessionMode(boolean hasJSimpleDB, boolean hasCoreAPI) {
        this.hasJSimpleDB = hasJSimpleDB;
        this.hasCoreAPI = hasCoreAPI;
    }

    /**
     * Determine whether the JSimpleDB (or "Java") API (e.g., {@link io.permazen.JTransaction}) is available in this mode.
     *
     * @return true if the JSimpleDB "Java" API is available
     */
    public boolean hasJSimpleDB() {
        return this.hasJSimpleDB;
    }

    /**
     * Determine whether the core API (e.g., {@link io.permazen.core.Transaction}) is available in this mode.
     *
     * @return true if the core API is available
     */
    public boolean hasCoreAPI() {
        return this.hasCoreAPI;
    }
}

