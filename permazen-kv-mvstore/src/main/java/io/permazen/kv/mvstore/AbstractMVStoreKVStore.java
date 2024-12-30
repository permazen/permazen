
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVStore;
import io.permazen.util.ByteData;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.Optional;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support superclass for {@link KVStore} implementations based on an open {@link MVStore}.
 *
 * <p>
 * Subclass must implement {@link #getMVMap}.
 */
@ThreadSafe
abstract class AbstractMVStoreKVStore extends MVMapKVStore {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    // Configuration info
    @GuardedBy("this")
    MVStore.Builder builder;
    @GuardedBy("this")
    String mapName;

    // Runtime info
    @GuardedBy("this")
    MVStore mvstore;

// Constructors

    protected AbstractMVStoreKVStore() {
    }

// Accessors

    /**
     * Configure the {@link MVStore.Builder} that will be used to construct the {@link MVStore} when {@code start()} is invoked.
     *
     * @param builder builder for the {@link MVStore}, or null to use a default builder
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setBuilder(MVStore.Builder builder) {
        Preconditions.checkState(this.mvstore == null, "already started");
        this.builder = builder;
    }

    /**
     * Configure the {@link MVStore.Builder} that will be used to construct the {@link MVStore} when {@code start()} is invoked
     * using the specified configuration string.
     *
     * @param builderConfig {@link MVStore.Builder} configuration string, or null to use a default builder
     * @throws IllegalStateException if this instance is already {@link #start}ed
     * @see MVStore.Builder#fromString
     */
    public synchronized void setBuilderConfig(String builderConfig) {
        Preconditions.checkState(this.mvstore == null, "already started");
        this.builder = builderConfig != null ? MVStore.Builder.fromString(builderConfig) : null;
    }

    /**
     * Configure the name of the {@link MVMap} to use.
     *
     * <p>
     * Default value is {@link MVStoreKVImplementation#DEFAULT_MAP_NAME}.
     *
     * @param mapName map name, or null to use {@link MVStoreKVImplementation#DEFAULT_MAP_NAME}
     * @throws IllegalStateException if this instance is already {@link #start}ed
     */
    public synchronized void setMapName(String mapName) {
        Preconditions.checkState(this.mvstore == null, "already started");
        this.mapName = mapName;
    }

    /**
     * Get the underlying {@link MVStore} associated with this instance.
     *
     * @return the associated underlying {@link MVStore}
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    public synchronized MVStore getMVStore() {
        Preconditions.checkState(this.mvstore != null, "not started");
        return this.mvstore;
    }

// MVMapKVStore

    /**
     * Get the underlying {@link MVMap} associated with this instance.
     *
     * @return the associated underlying {@link MVMap}
     * @throws IllegalStateException if this instance is not {@link #start}ed
     */
    @Override
    public abstract MVMap<ByteData, ByteData> getMVMap();

// Lifecycle

    /**
     * Start this instance.
     */
    @PostConstruct
    public synchronized void start() {

        // Already started?
        if (this.mvstore != null)
            return;
        this.log.info("starting {}", this);

        // Open MVStore and MVMap
        boolean success = false;
        try {
            this.doOpen();
            success = true;
        } finally {                                                         // clean up if error occurred
            if (!success && this.mvstore != null)
                this.doCloseImmediately();
        }
    }

    /**
     * Stop this instance.
     */
    @PreDestroy
    public synchronized void stop() {

        // Check state
        if (this.mvstore == null)
            return;
        this.log.info("stopping {}", this);

        // Close k/v store view
        this.doClose();
        this.mvstore = null;
    }

    protected void doOpen() {
        this.mvstore = Optional.ofNullable(this.builder).orElse(new MVStore.Builder()).open();
    }

    protected void doCloseImmediately() {
        this.mvstore.closeImmediately();
        this.mvstore = null;
    }

    protected void doClose() {
        this.mvstore.close();
        this.mvstore = null;
    }

// Object

    /**
     * Finalize this instance. Invokes {@link #stop} to close any unclosed iterators.
     */
    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable {
        try {
            if (this.mvstore != null)
               this.log.warn(this + " leaked without invoking stop()");
            this.stop();
        } finally {
            super.finalize();
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[map=\"" + this.mapName + "\""
          + "]";
    }
}
