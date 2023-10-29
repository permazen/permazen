
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import com.google.common.base.Preconditions;

import io.permazen.DefaultStorageIdGenerator;
import io.permazen.Permazen;
import io.permazen.PermazenFactory;
import io.permazen.StorageIdGenerator;
import io.permazen.core.Database;
import io.permazen.core.encoding.EncodingRegistry;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.simple.SimpleKVDatabase;

import java.util.Collection;

import org.springframework.beans.factory.config.AbstractFactoryBean;

/**
 * Factory bean created by <code>&lt;permazen:permazen&gt;</code> tags.
 */
class PermazenFactoryBean extends AbstractFactoryBean<Permazen> {

    private KVDatabase kvstore;
    private int schemaVersion = -1;
    private EncodingRegistry encodingRegistry;
    private StorageIdGenerator storageIdGenerator = new DefaultStorageIdGenerator();
    private Collection<Class<?>> modelClasses;

// Properties

    public void setKVStore(KVDatabase kvstore) {
        this.kvstore = kvstore;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public void setEncodingRegistry(EncodingRegistry encodingRegistry) {
        this.encodingRegistry = encodingRegistry;
    }

    public void setStorageIdGenerator(StorageIdGenerator storageIdGenerator) {
        this.storageIdGenerator = storageIdGenerator;
    }

    public void setModelClasses(Collection<Class<?>> modelClasses) {
        this.modelClasses = modelClasses;
    }

// InitializingBean

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        Preconditions.checkState(this.modelClasses != null, "no modelClasss configured");
    }

// AbstractFactoryBean

    @Override
    public Class<?> getObjectType() {
        return Permazen.class;
    }

    @Override
    protected Permazen createInstance() {

        // Apply defaults
        KVDatabase kvstore1 = this.kvstore;
        int schemaVersion1 = this.schemaVersion;
        if (kvstore1 == null)
            kvstore1 = new SimpleKVDatabase();
        final Database db = new Database(kvstore1);

        // Build Permazen
        return new PermazenFactory()
          .setDatabase(db)
          .setSchemaVersion(schemaVersion1)
          .setEncodingRegistry(this.encodingRegistry)
          .setStorageIdGenerator(this.storageIdGenerator)
          .setModelClasses(this.modelClasses)
          .newPermazen();
    }
}
