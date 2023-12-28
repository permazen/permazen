
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.core.CounterField;
import io.permazen.core.Field;
import io.permazen.core.FieldSwitch;
import io.permazen.core.ListField;
import io.permazen.core.MapField;
import io.permazen.core.ObjType;
import io.permazen.core.Schema;
import io.permazen.core.SchemaBundle;
import io.permazen.core.SetField;
import io.permazen.core.SimpleField;
import io.permazen.kv.KVStore;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Runtime information used by {@link Jsck}.
 */
class JsckInfo implements JsckLogger {

    private final JsckConfig config;
    private final KVStore kv;
    private final AtomicLong counter = new AtomicLong();
    private final Set<Storage<?>> storages = new HashSet<>();
    private final Consumer<? super Issue> handler;

    private SchemaBundle schemaBundle;
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

    public int getFormatVersion() {
        return this.formatVersion;
    }
    public void setFormatVersion(int formatVersion) {
        this.formatVersion = formatVersion;
    }

    public SchemaBundle getSchemaBundle() {
        return this.schemaBundle;
    }
    public void setSchemaBundle(SchemaBundle schemaBundle) {
        this.schemaBundle = schemaBundle;
    }

    public Set<Storage<?>> getStorages() {
        return this.storages;
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

    // Inventory all storages
    void inventoryStorages() {
        for (Schema schema : this.schemaBundle.getSchemasBySchemaId().values()) {
            for (ObjType objType : schema.getObjTypes().values())
                this.inventoryStorages(objType);
        }
    }

    // Inventory all storages associated with the given object type
    private void inventoryStorages(ObjType objType) {

        // Add storage for object type
        this.storages.add(new ObjectType(this, objType));

        // Add storage for field indexes
        for (Field<?> field : objType.getFields().values()) {
            field.visit(new FieldSwitch<Void>() {

                @Override
                public <T> Void caseSimpleField(SimpleField<T> field) {
                    if (field.isIndexed()) {
                        JsckInfo.this.storages.add(
                          new SimpleFieldIndex<T, io.permazen.core.SimpleFieldIndex<T>>(JsckInfo.this, field));
                    }
                    return null;
                }

                @Override
                public <E> Void caseSetField(SetField<E> field) {
                    if (field.getElementField().isIndexed())
                        JsckInfo.this.storages.add(new SetElementIndex<>(JsckInfo.this, field));
                    return null;
                }

                @Override
                public <E> Void caseListField(ListField<E> field) {
                    if (field.getElementField().isIndexed())
                        JsckInfo.this.storages.add(new ListElementIndex<>(JsckInfo.this, field));
                    return null;
                }

                @Override
                public <K, V> Void caseMapField(MapField<K, V> field) {
                    if (field.getKeyField().isIndexed())
                        JsckInfo.this.storages.add(new MapKeyIndex<>(JsckInfo.this, field));
                    if (field.getValueField().isIndexed())
                        JsckInfo.this.storages.add(new MapValueIndex<>(JsckInfo.this, field));
                    return null;
                }

                @Override
                public Void caseCounterField(CounterField field) {
                    return null;
                }
            });
        }

        // Add storage for composite indexes
        for (io.permazen.core.CompositeIndex index : objType.getCompositeIndexes().values())
            this.storages.add(new CompositeIndex(this, index));
    }
}
