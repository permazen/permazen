
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.dao;

import java.util.List;

import javax.persistence.FlushModeType;

/**
 * Data Access Object (DAO) generic interface.
 */
public interface DAO<T> {

// Access methods

    /**
     * Get an instance by ID. This assumes object IDs are long values.
     */
    T getById(long id);

    /**
     * Get all instances.
     */
    List<T> getAll();

    /**
     * Get a reference to an instance by ID. This assumes object IDs are long values.
     *
     * <p>
     * Note if the instance does not exist, then an exception may be thrown either here or later upon first access.
     */
    T getReference(long id);

// Lifecycle methods

    /**
     * Save a newly created instance.
     */
    void save(T obj);

    /**
     * Delete the given instance from the persistent store.
     */
    void delete(T obj);

    /**
     * Merge the given object into the current session.
     */
    T merge(T obj);

    /**
     * Refresh the given object from the database.
     */
    void refresh(T obj);

    /**
     * Evict an object from the session cache.
     */
    void detach(T obj);

// Session methods

    /**
     * Flush outstanding changes to the persistent store.
     */
    void flush();

    /**
     * Set flush mode.
     */
    void setFlushMode(FlushModeType flushMode);

    /**
     * Clear the session cache.
     */
    void clear();

    /**
     * Determine if the current transaction is read-only.
     *
     * @return true if the current transaction is read-only
     * @throws IllegalStateException if no transaction is associated with the current thread
     */
    boolean isReadOnly();
}

