
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import com.google.common.base.Preconditions;

import io.permazen.Permazen;
import io.permazen.PermazenConfig;
import io.permazen.core.Database;
import io.permazen.encoding.EncodingRegistry;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.simple.SimpleKVDatabase;

import java.util.Collection;

import org.springframework.beans.factory.config.AbstractFactoryBean;

/**
 * Factory bean created by <code>&lt;permazen:permazen&gt;</code> tags.
 */
class PermazenFactoryBean extends AbstractFactoryBean<Permazen> {

    private KVDatabase kvstore;
    private EncodingRegistry encodingRegistry;
    private Collection<Class<?>> modelClasses;

// Properties

    public void setKVStore(KVDatabase kvstore) {
        this.kvstore = kvstore;
    }

    public void setEncodingRegistry(EncodingRegistry encodingRegistry) {
        this.encodingRegistry = encodingRegistry;
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

        // Build underlying database
        final Database db = new Database(this.kvstore != null ? this.kvstore : new SimpleKVDatabase());

        // Build Permazen
        return PermazenConfig.builder()
          .database(db)
          .encodingRegistry(this.encodingRegistry)
          .modelClasses(this.modelClasses)
          .build()
          .newPermazen();
    }
}
