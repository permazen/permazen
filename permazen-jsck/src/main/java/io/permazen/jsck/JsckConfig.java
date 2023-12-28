
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import com.google.common.base.Preconditions;

import io.permazen.encoding.DefaultEncodingRegistry;
import io.permazen.encoding.EncodingRegistry;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;

import java.util.Map;

import org.slf4j.LoggerFactory;

/**
 * Configuration for a {@link Jsck} key/value database consistency inspection.
 */
public class JsckConfig {

    private JsckLogger logger = JsckLogger.wrap(LoggerFactory.getLogger(this.getClass()));
    private EncodingRegistry encodingRegistry = new DefaultEncodingRegistry();
    private boolean garbageCollectSchemas;
    private int formatVersionOverride;
    private Map<Integer, SchemaModel> schemaOverrides;
    private Map<Integer, SchemaId> storageIdOverrides;
    private long maxIssues = Long.MAX_VALUE;
    private boolean repair;

    /**
     * Get the {@link EncodingRegistry} used to interpret encoding names in recorded schemas.
     *
     * <p>
     * Any custom encodings used to encode fields in the database must be included in a configured {@link EncodingRegistry}.
     *
     * <p>
     * Default is {@code new EncodingRegistry()}.
     *
     * @return registry of encodings
     */
    public EncodingRegistry getEncodingRegistry() {
        return this.encodingRegistry;
    }
    public void setEncodingRegistry(EncodingRegistry encodingRegistry) {
        Preconditions.checkArgument(encodingRegistry != null, "null encodingRegistry");
        this.encodingRegistry = encodingRegistry;
    }

    /**
     * Determine whether to garbage collect unused schema versions.
     *
     * <p>
     * If set, at the end of inspection any unused schema versions will be deleted.
     * Note this occurs even if {@link #isRepair} returns false.
     *
     * <p>
     * Default false.
     *
     * @return true to garbage collect unused schema versions, otherwise false
     */
    public boolean isGarbageCollectSchemas() {
        return this.garbageCollectSchemas;
    }
    public void setGarbageCollectSchemas(boolean garbageCollectSchemas) {
        this.garbageCollectSchemas = garbageCollectSchemas;
    }

    /**
     * Configure a destination for log messages emitted during the scan.
     *
     * <p>
     * By default, messages are logged via this class' {@link org.slf4j.Logger} via {@link JsckLogger#wrap JsckLogger.wrap()}.
     *
     * @return true to repair inconsistencies, otherwise false
     */
    public JsckLogger getJsckLogger() {
        return this.logger;
    }
    public void setJsckLogger(JsckLogger logger) {
        this.logger = logger;
    }

    /**
     * Determine whether to repair any database inconsistencies found.
     *
     * <p>
     * Without this flag being set, no changes will be made to the key/value store, with the exception of
     * {@linkplain #isGarbageCollectSchemas garbage collecting schema versions}.
     *
     * <p>
     * Default false.
     *
     * @return true to repair inconsistencies, otherwise false
     */
    public boolean isRepair() {
        return this.repair;
    }
    public void setRepair(boolean repair) {
        this.repair = repair;
    }

    /**
     * Configure schemas to forcibly apply into the schema table.
     *
     * <p>
     * Entries in the configured {@link Map} will be forcibly written to the database, causing any existing schemas
     * recorded under the same schema index to be overridden.
     *
     * <p>
     * Entries with null values will <i>forcibly delete</i> the corresponding schemas and all objects having that
     * schema version from the database.
     *
     * <p>
     * <b>Warning</b>: Use of this property is dangerous and for experts only.
     *
     * <p>
     * Default is an empty map (i.e., disabled).
     *
     * @return map from schema index to schema model (to add/override schemas) or null (to remove schemas)
     */
    public Map<Integer, SchemaModel> getSchemaOverrides() {
        return this.schemaOverrides;
    }
    public void setSchemaOverrides(Map<Integer, SchemaModel> schemaOverrides) {
        this.schemaOverrides = schemaOverrides;
    }

    /**
     * Configure {@link SchemaId}s to forcibly apply into the storage ID table.
     *
     * <p>
     * Entries in the configured {@link Map} will be forcibly written to the database, causing any existing schema ID's
     * recorded under the same storage ID to be overridden.
     *
     * <p>
     * Entries with null values will <i>forcibly delete</i> the corresponding storage ID table entry from the database.
     *
     * <p>
     * <b>Warning</b>: Use of this property is dangerous and for experts only.
     *
     * <p>
     * Default is an empty map (i.e., disabled).
     *
     * @return map from storage ID to schema ID (to add/override schema IDs) or null (to remove schema IDs)
     */
    public Map<Integer, SchemaId> getStorageIdOverrides() {
        return this.storageIdOverrides;
    }
    public void setStorageIdOverrides(Map<Integer, SchemaId> storageIdOverrides) {
        this.storageIdOverrides = storageIdOverrides;
    }

    /**
     * Configure a database format version to forcibly apply.
     *
     * <p>
     * Using this property will cause any existing database format version number to be overridden with the configured value.
     *
     * <p>
     * <b>Warning</b>: Use of this property is dangerous and for experts only.
     *
     * <p>
     * Default zero (i.e., disabled).
     *
     * @return enforced format version, or zero if disabled
     */
    public int getFormatVersionOverride() {
        return this.formatVersionOverride;
    }
    public void setFormatVersionOverride(int formatVersionOverride) {
        this.formatVersionOverride = formatVersionOverride;
    }

    /**
     * Configure a maximum number of issues to generate.
     *
     * <p>
     * Default is {@link Long#MAX_VALUE}.
     *
     * @return maximum number of issues
     */
    public long getMaxIssues() {
        return this.maxIssues;
    }
    public void setMaxIssues(long maxIssues) {
        Preconditions.checkArgument(maxIssues >= 0, "maxIssues < 0");
        this.maxIssues = maxIssues;
    }
}
