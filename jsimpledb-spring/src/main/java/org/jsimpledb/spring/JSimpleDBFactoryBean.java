
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.spring;

import com.google.common.base.Preconditions;

import org.jsimpledb.DefaultStorageIdGenerator;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JSimpleDBFactory;
import org.jsimpledb.StorageIdGenerator;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
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
        Preconditions.checkState(this.modelClasses != null, "no modelClasss configured");
    }

// AbstractFactoryBean

    @Override
    public Class<?> getObjectType() {
        return JSimpleDB.class;
    }

    @Override
    protected JSimpleDB createInstance() {

        // Apply defaults
        KVDatabase kvstore1 = this.kvstore;
        int schemaVersion1 = this.schemaVersion;
        if (kvstore1 == null)
            kvstore1 = new SimpleKVDatabase();
        final Database db = new Database(kvstore1);

        // Add custom field types
        if (this.fieldTypeClasses != null)
            db.getFieldTypeRegistry().addClasses(this.fieldTypeClasses);

        // Build JSimpleDB
        return new JSimpleDBFactory()
          .setDatabase(db)
          .setSchemaVersion(schemaVersion1)
          .setStorageIdGenerator(this.storageIdGenerator)
          .setModelClasses(this.modelClasses)
          .newJSimpleDB();
    }
}

