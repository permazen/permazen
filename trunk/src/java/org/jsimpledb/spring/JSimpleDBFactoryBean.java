
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

import org.jsimpledb.DefaultStorageIdGenerator;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JSimpleDBFactory;
import org.jsimpledb.StorageIdGenerator;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.kv.KVDatabase;
import org.springframework.beans.factory.config.AbstractFactoryBean;

/**
 * Factory bean created by <code>&lt;jsimpledb:jsimpledb&gt;</code> tags.
 */
class JSimpleDBFactoryBean extends AbstractFactoryBean<JSimpleDB> {

    private KVDatabase kvstore;
    private int schemaVersion = -1;
    private StorageIdGenerator storageIdGenerator = new DefaultStorageIdGenerator();
    private Iterable<? extends Class<?>> modelClasses;
    private Iterable<? extends Class<? extends FieldType<?>>> fieldTypeClasses;

// Properties

    public void setKVStore(KVDatabase kvstore) {
        this.kvstore = kvstore;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public void setStorageIdGenerator(StorageIdGenerator storageIdGenerator) {
        this.storageIdGenerator = storageIdGenerator;
    }

    public void setModelClasses(Iterable<? extends Class<?>> modelClasses) {
        this.modelClasses = modelClasses;
    }

    public void setFieldTypeClasses(Iterable<? extends Class<? extends FieldType<?>>> fieldTypeClasses) {
        this.fieldTypeClasses = fieldTypeClasses;
    }

// InitializingBean

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (this.kvstore == null)
            throw new IllegalStateException("no kvstore configured");
        if (this.schemaVersion == -1)
            throw new IllegalStateException("no schemaVersion configured");
        if (this.modelClasses == null)
            throw new IllegalStateException("no modelClasses configured");
    }

// AbstractFactoryBean

    @Override
    public Class<?> getObjectType() {
        return JSimpleDB.class;
    }

    @Override
    protected JSimpleDB createInstance() {

        // Build and configure database
        final Database db = new Database(this.kvstore);
        if (this.fieldTypeClasses != null)
            db.getFieldTypeRegistry().addClasses(this.fieldTypeClasses);

        // Build JSimpleDB
        final JSimpleDBFactory factory = new JSimpleDBFactory();
        return factory
          .setDatabase(db)
          .setSchemaVersion(this.schemaVersion)
          .setStorageIdGenerator(this.storageIdGenerator)
          .setModelClasses(this.modelClasses)
          .newJSimpleDB();
    }
}

