
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.jsck;

import com.google.common.base.Preconditions;

import java.util.Map;

import org.jsimpledb.core.FieldTypeRegistry;
import org.jsimpledb.core.Layout;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.schema.SchemaModel;
import org.slf4j.LoggerFactory;

/**
 * Configuration for a {@link Jsck} key/value database consistency inspection.
 */
public class JsckConfig {

    private JsckLogger logger = JsckLogger.wrap(LoggerFactory.getLogger(this.getClass()));
    private KeyRanges keysToInspect;
    private FieldTypeRegistry fieldTypeRegistry = new FieldTypeRegistry();
    private boolean garbageCollectSchemas;
    private Map<Integer, SchemaModel> forceSchemaVersions;
    private int forceFormatVersion;
    private long maxIssues = Long.MAX_VALUE;
    private boolean repair;

    /**
     * Get the restricted ranges of keys that should be inspected, if any.
     *
     * <p>
     * This property allows limiting inspection to specific objects, object types, or indexes.
     * Note: recorded schema information is always fully checked.
     *
     * <p>
     * Default is null, i.e., no restriction.
     *
     * @return ranges of keys to inspect, or null for no restriction
     */
    public KeyRanges getKeysToInspect() {
        return this.keysToInspect;
    }
    public void setKeysToInspect(KeyRanges keysToInspect) {
        this.keysToInspect = keysToInspect;
    }

    /**
     * Get the {@link FieldTypeRegistry} used to interpret field type names in recorded schemas.
     *
     * <p>
     * Any custom field types used to encode fields in the database must be included in a configured {@link FieldTypeRegistry}.
     *
     * <p>
     * Default is {@code new FieldTypeRegistry()}.
     *
     * @return registry of field types
     */
    public FieldTypeRegistry getFieldTypeRegistry() {
        return this.fieldTypeRegistry;
    }
    public void setFieldTypeRegistry(FieldTypeRegistry fieldTypeRegistry) {
        Preconditions.checkArgument(keysToInspect != null, "null keysToInspect");
        this.fieldTypeRegistry = fieldTypeRegistry;
    }

    /**
     * Determine whether to garbage collect unused schema versions.
     *
     * <p>
     * If set, at the end of inspection any unused schema versions will be deleted.
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
     * By default, messages are logged via this class' {@link Logger} via {@link JsckLogger#wrap JsckLogger.wrap()}.
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
     * Determine whether to repair inconsistencies found.
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
     * Configure schema versions to forcibly apply.
     *
     * <p>
     * Entries in the configured {@link Map} will be forcibly written to the database, causing any existing schema versions
     * recorded under the same version number to be overridden. Otherwise, if a valid recorded schema does not have a
     * corresponding entry in this map, it is assumed by {@link Jsck} to be accurate.
     *
     * <p>
     * Entries with null values will <i>forcibly delete</i> the corresponding schema version and all objects having that
     * schema version from the database.
     *
     * <p>
     * <b>Warning</b>: use of this property is dangerous, because it affects how {@link Jsck} interprets the key/value
     * data of objects with the corresponding version(s) in the database. This property should only be used if:
     * <ul>
     *  <li>It is known that the corresponding schema entries recorded in the key/value database have
     *      been somehow corrupted or deleted; and</li>
     *  <li>The actual schema version(s) are known and configured here.</li>
     * </ul>
     *
     * <p>
     * Default is an empty map (i.e., disabled).
     *
     * @return map from schema version number to schema model (to add/override schema version) or null (to remove schema version)
     */
    public Map<Integer, SchemaModel> getForceSchemaVersions() {
        return this.forceSchemaVersions;
    }
    public void setForceSchemaVersions(Map<Integer, SchemaModel> forceSchemaVersions) {
        this.forceSchemaVersions = forceSchemaVersions;
    }

    /**
     * Configure a database format version to forcibly apply.
     *
     * <p>
     * Using this property will cause any existing database format version number to be overridden with the configured value.
     *
     * <p>
     * <b>Warning</b>: use of this property is dangerous, because it affects how {@link Jsck} interprets all other key/value
     * data in the database. This property should only be used if:
     * <ul>
     *  <li>This {@link Jsck} utility is at least as current as the any version of JSimpleDB that has written to the database</li>
     *  <li>It is known that the {@linkplain Layout#getFormatVersionKey format version key/value entry}
     *      has been somehow corrupted or deleted; and</li>
     *  <li>The actual format version is known and configured here.</li>
     * </ul>
     *
     * <p>
     * In other words, "only use this if you know what you are doing".
     *
     * <p>
     * Default zero (i.e., disabled).
     *
     * @return enforced format version, or zero if disabled
     */
    public int getForceFormatVersion() {
        return this.forceFormatVersion;
    }
    public void setForceFormatVersion(int forceFormatVersion) {
        Preconditions.checkArgument(forceFormatVersion >= 0, "forceFormatVersion < 0");
        Preconditions.checkArgument(forceFormatVersion <= Layout.CURRENT_FORMAT_VERSION,
          "unrecognized forceFormatVersion > " + Layout.CURRENT_FORMAT_VERSION);
        this.forceFormatVersion = forceFormatVersion;
    }

    /**
     * Configure a maximum number of issues to generate.
     *
     * <p>
     * Default is {@link Long.MAX_VALUE}.
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
