
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.FieldType;
import io.permazen.core.type.EnumValueFieldType;
import io.permazen.core.type.ReferenceFieldType;
import io.permazen.kv.KVStore;
import io.permazen.schema.CounterSchemaField;
import io.permazen.schema.EnumSchemaField;
import io.permazen.schema.ListSchemaField;
import io.permazen.schema.MapSchemaField;
import io.permazen.schema.ReferenceSchemaField;
import io.permazen.schema.SchemaCompositeIndex;
import io.permazen.schema.SchemaField;
import io.permazen.schema.SchemaFieldSwitch;
import io.permazen.schema.SchemaModel;
import io.permazen.schema.SchemaObjectType;
import io.permazen.schema.SetSchemaField;
import io.permazen.schema.SimpleSchemaField;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Runtime information used by {@link Jsck}.
 */
class JsckInfo implements JsckLogger {

    private final JsckConfig config;
    private final KVStore kv;
    private final AtomicLong counter = new AtomicLong();
    private final Map<Integer, SchemaModel> schemas = new HashMap<>();              // version -> SchemaModel
    private final Map<Integer, Map<Integer, Storage>> storages = new HashMap<>();   // version -> (storage ID -> Storage)
    private final Map<Integer, Index> indexes = new HashMap<>();                    // storage ID -> Index
    private final Consumer<? super Issue> handler;

    private int formatVersion;

    JsckInfo(JsckConfig config, KVStore kv, Consumer<? super Issue> handler) {
        this.config = config;
        this.kv = kv;
        this.handler = handler;
        if (this.config.getMaxIssues() <= 0)
            throw new MaxIssuesReachedException();
    }

    public JsckConfig getConfig() {
        return this.config;
    }

    public KVStore getKVStore() {
        return this.kv;
    }

    public Map<Integer, SchemaModel> getSchemas() {
        return this.schemas;
    }

    public Map<Integer, Map<Integer, Storage>> getStorages() {
        return this.storages;
    }

    public Map<Integer, Index> getIndexes() {
        return this.indexes;
    }

    public int getFormatVersion() {
        return this.formatVersion;
    }
    public void setFormatVersion(int formatVersion) {
        this.formatVersion = formatVersion;
    }

    // Handle an issue
    public void handle(Issue issue) {
        this.handle(issue, false);
    }

    // Handle an issue, with optional forced repair
    public void handle(Issue issue, boolean force) {
        if (this.config.isRepair() || force)
            issue.apply(this.kv);
        if (this.handler != null)
            this.handler.accept(issue);
        if (this.counter.incrementAndGet() >= this.config.getMaxIssues())
            throw new MaxIssuesReachedException();
    }

    public long getNumberOfIssuesHandled() {
        return this.counter.get();
    }

// JsckLogger

    @Override
    public boolean isDetailEnabled() {
        final JsckLogger logger = this.config.getJsckLogger();
        return logger != null && logger.isDetailEnabled();
    }

    @Override
    public void info(String message) {
        final JsckLogger logger = this.config.getJsckLogger();
        if (logger != null)
            logger.info(message);
    }

    @Override
    public void detail(String message) {
        final JsckLogger logger = this.config.getJsckLogger();
        if (logger != null && logger.isDetailEnabled())
            logger.detail(message);
    }

// Internal stuff

    // Inventory all storage ID's
    void inventoryStorages() {
        for (Map.Entry<Integer, SchemaModel> entry : this.schemas.entrySet())
            this.inventoryStorages(entry.getKey(), entry.getValue());
    }

    private void inventoryStorages(int schemaVersion, SchemaModel schema) {
        for (SchemaObjectType objectType : schema.getSchemaObjectTypes().values())
            this.inventoryStorages(schemaVersion, objectType);
    }

    private void inventoryStorages(final int schemaVersion, SchemaObjectType objectType) {

        // Add storage for object type
        this.addStorage(schemaVersion, new ObjectType(this, objectType));

        // Add storage for field indexes
        for (SchemaField field : objectType.getSchemaFields().values()) {
            field.visit(new SchemaFieldSwitch<Void>() {

                @Override
                public Void caseSimpleSchemaField(SimpleSchemaField field) {
                    if (field.isIndexed())
                        JsckInfo.this.addStorage(schemaVersion, new SimpleFieldIndex(JsckInfo.this, schemaVersion, field));
                    return null;
                }

                @Override
                public Void caseSetSchemaField(SetSchemaField field) {
                    if (field.getElementField().isIndexed())
                        JsckInfo.this.addStorage(schemaVersion, new SetElementIndex(JsckInfo.this, schemaVersion, field));
                    return null;
                }

                @Override
                public Void caseListSchemaField(ListSchemaField field) {
                    if (field.getElementField().isIndexed())
                        JsckInfo.this.addStorage(schemaVersion, new ListElementIndex(JsckInfo.this, schemaVersion, field));
                    return null;
                }

                @Override
                public Void caseMapSchemaField(MapSchemaField field) {
                    if (field.getKeyField().isIndexed())
                        JsckInfo.this.addStorage(schemaVersion, new MapKeyIndex(JsckInfo.this, schemaVersion, field));
                    if (field.getValueField().isIndexed())
                        JsckInfo.this.addStorage(schemaVersion, new MapValueIndex(JsckInfo.this, schemaVersion, field));
                    return null;
                }

                @Override
                public Void caseCounterSchemaField(CounterSchemaField field) {
                    return null;
                }
            });
        }

        // Add storage for composite indexes
        for (SchemaCompositeIndex index : objectType.getSchemaCompositeIndexes().values())
            this.addStorage(schemaVersion, new CompositeIndex(this, schemaVersion, objectType, index));
    }

    // Find FieldType for field
    FieldType<?> findFieldType(final int schemaVersion, final SimpleSchemaField schemaField) {
        return schemaField.visit(new SchemaFieldSwitch<FieldType<?>>() {

            @Override
            public FieldType<?> caseEnumSchemaField(EnumSchemaField field) {
                return new EnumValueFieldType(field.getIdentifiers());
            }

            @Override
            public FieldType<?> caseReferenceSchemaField(ReferenceSchemaField field) {
                return new ReferenceFieldType(field.getObjectTypes());
            }

            @Override
            public FieldType<?> caseSimpleSchemaField(SimpleSchemaField field) {
                final FieldType<?> fieldType = JsckInfo.this.config.getFieldTypeRegistry().getFieldType(field.getEncodingId());
                if (fieldType == null) {
                    throw new IllegalArgumentException("no field encoding \"" + field.getEncodingId() + "\""
                      + " (used by " + field + " in schema version " + schemaVersion
                      + ") was found in the configured FieldTypeRepository");
                }
                return fieldType;
            }
        });
    }

    // Add new Storage, checking for conflicts
    private void addStorage(final int schemaVersion, final Storage storage) {

        // Set schema version
        storage.setSchemaVersion(schemaVersion);

        // Double-check compatibility
        final int storageId = storage.getStorageId();
        for (Map<Integer, Storage> map : this.storages.values()) {
            final Storage other = map.get(storageId);
            assert other == null || !(storage instanceof ObjectType) || !(other instanceof ObjectType);
            if (other != null && !storage.isCompatible(other)) {
                throw new IllegalArgumentException("schemas conflict for storage ID " + storageId
                  + ":\n  in schema version " + other.getSchemaVersion() + ": " + other
                  + ":\n  in schema version " + storage.getSchemaVersion() + ": " + storage);
            }
        }

        // Add storage
        this.storages.computeIfAbsent(schemaVersion, v -> new HashMap<>()).put(storageId, storage);

        // Add index
        if (storage instanceof Index)
            this.indexes.put(storageId, (Index)storage);
    }
}
