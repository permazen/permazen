
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import io.permazen.JTransaction;
import io.permazen.Permazen;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.EnumValue;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;

/**
 * Console {@link Session} modes.
 *
 * <p>
 * The three {@link SessionMode} values correspond to the three layers of abstraction in Permazen.
 * In order from bottom to top these are:
 * <ul>
 *  <li>The key/value database layer, providing {@link KVDatabase}
 *      and {@link KVTransaction} and represented by {@link #KEY_VALUE}</li>
 *  <li>The core API database layer, providing {@link Database}
 *      and {@link Transaction} and represented by {@link #CORE_API}</li>
 *  <li>The Permazen Java layer, providing {@link Permazen}
 *      and {@link JTransaction} and represented by {@link #PERMAZEN}</li>
 * </ul>
 */
public enum SessionMode {

    /**
     * Permazen key/value database mode.
     *
     * <p>
     * This is the lowest level CLI mode, where only the raw {@code byte[]} key/value store is available.
     * This mode provides a {@link KVTransaction}, but no {@link Transaction} or {@link JTransaction}.
     *
     * @see KVDatabase
     */
    KEY_VALUE(false, false),

    /**
     * Permazen Core API mode.
     *
     * <p>
     * Supports access to low level schema information, core API "objects" and fields, but no Java-specific operations.
     * This mode provides a {@link KVTransaction} and {@link Transaction}, but no {@link JTransaction}.
     *
     * <p>
     * Schema information may be provided either by a schema XML file or by
     * {@link PermazenType &#64;PermazenType}-annotated Java model classes; however, in the latter
     * case these classes will be otherwise ignored.
     *
     * <p>
     * In this mode, database objects are represented by {@link ObjId}'s and {@code enum} values by {@link EnumValue}'s.
     */
    CORE_API(false, true),

    /**
     * Permazen Java mode.
     *
     * <p>
     * Provides database access through standard Java model objects and transaction access via {@link JTransaction}.
     * Requires and provides a {@link Permazen} instance and its associated Java model classes.
     *
     * <p>
     * In this mode, database objects are represented by Java model class instances and {@code enum} values by corresponding
     * Java model {@link Enum} class instances.
     */
    PERMAZEN(true, true);

    private final boolean hasPermazen;
    private final boolean hasCoreAPI;

    SessionMode(boolean hasPermazen, boolean hasCoreAPI) {
        this.hasPermazen = hasPermazen;
        this.hasCoreAPI = hasCoreAPI;
    }

    /**
     * Determine whether the Permazen Java API (e.g., {@link JTransaction}) based on a {@link Permazen}
     * instance is available in this mode.
     *
     * @return true if the Permazen Java API is available
     */
    public boolean hasPermazen() {
        return this.hasPermazen;
    }

    /**
     * Determine whether the core API (e.g., {@link Transaction}) is available in this mode.
     *
     * @return true if the core API is available
     */
    public boolean hasCoreAPI() {
        return this.hasCoreAPI;
    }
}
