
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import io.permazen.core.Encodings;
import io.permazen.core.Field;
import io.permazen.core.InvalidSchemaException;
import io.permazen.core.Layout;
import io.permazen.core.ObjId;
import io.permazen.core.Schema;
import io.permazen.core.SchemaBundle;
import io.permazen.core.SchemaMismatchException;
import io.permazen.core.UnknownFieldException;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.ParseContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Applies consistency checks to, and optionally repairs corruption of, a Permazen key/value database.
 */
public class Jsck {

    private static final int HEX_STRING_LIMIT = 100;

    private final JsckConfig config;

    public Jsck(JsckConfig config) {
        Preconditions.checkArgument(config != null, "null config");
        this.config = config;
    }

    /**
     * Perform check.
     *
     * @param kv key/value database
     * @param consumer recipient of any issues found; or null for none
     * @return the number of issues encountered
     * @throws IllegalArgumentException if database is not a Permazen database
     *  (i.e., no {@linkplain Layout#getFormatVersionKey format version key})
     * @throws IllegalArgumentException if an {@linkplain JsckConfig#getSchemaOverrides override schema} is invalid
     * @throws SchemaMismatchException if the database's recorded schemas are mutually inconsistent (due to schema overrides)
     */
    public long check(KVStore kv, Consumer<? super Issue> consumer) {
        final JsckInfo info;
        try {
            info = new JsckInfo(this.config, kv, consumer);
        } catch (MaxIssuesReachedException e) {
            return 0;
        }
        try {
            this.doCheck(info);
        } catch (MaxIssuesReachedException e) {
            // ignore
        }
        return info.getNumberOfIssuesHandled();
    }

    private void doCheck(JsckInfo info) {

        // Get key prefixes
        final ByteData formatVersionKey = Layout.getFormatVersionKey();
        final ByteData schemaTablePrefix = Layout.getSchemaTablePrefix();
        final ByteData storageIdTablePrefix = Layout.getStorageIdTablePrefix();
        final ByteData schemaIndexKeyPrefix = Layout.getSchemaIndexKeyPrefix();
        final ByteData userMetaDataKeyPrefix = Layout.getUserMetaDataKeyPrefix();

        assert Layout.METADATA_PREFIX_BYTE == 0;
        assert schemaTablePrefix.byteAt(0) == Layout.METADATA_PREFIX_BYTE;
        assert schemaIndexKeyPrefix.byteAt(0) == Layout.METADATA_PREFIX_BYTE;
        assert storageIdTablePrefix.byteAt(0) == Layout.METADATA_PREFIX_BYTE;
        assert userMetaDataKeyPrefix.byteAt(0) == Layout.METADATA_PREFIX_BYTE;

        assert schemaTablePrefix.compareTo(formatVersionKey) > 0;
        assert storageIdTablePrefix.compareTo(schemaTablePrefix) > 0;
        assert schemaIndexKeyPrefix.compareTo(storageIdTablePrefix) > 0;
        assert userMetaDataKeyPrefix.compareTo(schemaIndexKeyPrefix) > 0;

        // Check empty space before format version
        this.checkEmpty(info, new KeyRange(ByteData.empty(), formatVersionKey), "the key range prior to format version");

        // Check format version
        info.info("checking the format version under key %s", Jsck.ds(formatVersionKey));
        int formatVersionOverride = this.config.getFormatVersionOverride();
        if (formatVersionOverride < 0 || formatVersionOverride > Layout.CURRENT_FORMAT_VERSION) {
            throw new IllegalArgumentException(String.format(
              "invalid format version override: %d is not in the range 1..%d",
              formatVersionOverride, Layout.CURRENT_FORMAT_VERSION));
        }
        final KVStore kv = info.getKVStore();
        ByteData val = kv.get(formatVersionKey);
        try {

            // Read and validate format version
            if (val == null) {
                throw new IllegalArgumentException(kv.getAtLeast(ByteData.empty(), null) == null ? "database is empty" :
                  "missing Permazen signature/format version key " + Jsck.ds(formatVersionKey));
            }
            final ByteData.Reader reader = val.newReader();
            final int formatVersion;
            try {
                formatVersion = Encodings.UNSIGNED_INT.read(reader);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(
                  "invalid Permazen signature/format version value %s: can't decode version number", Jsck.ds(val)));
            }
            if (reader.remain() > 0) {
                throw new IllegalArgumentException(String.format(
                  "invalid Permazen signature/format version value %s: trailing garbage %s follows format version number %d",
                  Jsck.ds(val), Jsck.ds(reader, reader.getOffset()), info.getFormatVersion()));
            }
            info.setFormatVersion(formatVersion);
            switch (formatVersion) {
            case Layout.FORMAT_VERSION_1:
                break;
            default:
                throw new IllegalArgumentException(String.format(
                  "invalid Permazen signature/format version key value %s: unrecognized format version number %d",
                  Jsck.ds(val), formatVersion));
            }
            info.info("database format version is %d (current format version is %d)", formatVersion, Layout.CURRENT_FORMAT_VERSION);

            // Override format version, if needed
            if (formatVersionOverride != 0 && formatVersionOverride != info.getFormatVersion()) {
                final ByteData newValue = Encodings.UNSIGNED_INT.encode(formatVersionOverride);
                info.handle(new InvalidValue(formatVersionKey, newValue).setDetail(
                  "forcibly override format version %d with override version %d", info.getFormatVersion(), formatVersionOverride));
            }
        } catch (IllegalArgumentException e) {
            if (formatVersionOverride == 0)
                throw e;
            info.setFormatVersion(formatVersionOverride);
            final ByteData newValue = Encodings.UNSIGNED_INT.encode(formatVersionOverride);
            info.handle(new InvalidValue(formatVersionKey, val, newValue).setDetail("%s", e.getMessage()));
        }

        // Check empty space before schema table
        this.checkEmpty(info, new KeyRange(ByteUtil.getNextKey(formatVersionKey), schemaTablePrefix),
          "the key range between format version and schema table");

        // Check the schema table
        final Function<ByteData, SchemaModel> schemaDecoder = bytes -> {

            // Decode schema model
            final SchemaModel schema;
            try {
                schema = Layout.decodeSchema(bytes);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("invalid encoded schema: %s", e.getMessage()), e);
            } catch (InvalidSchemaException e) {
                throw new IllegalArgumentException(String.format("invalid schema model: %s", e.getMessage()), e);
            }
            schema.lockDown(true);

            // Validate schema model
            try {
                schema.validateWithEncodings(this.config.getEncodingRegistry());
            } catch (InvalidSchemaException e) {
                throw new IllegalArgumentException(String.format("schema invalid: %s", e.getMessage()), e);
            }

            // Done
            return schema;
        };
        final Function<SchemaModel, ByteData> schemaEncoder = schema -> {
            final ByteData.Writer writer = ByteData.newWriter();
            Layout.encodeSchema(writer, schema);
            return writer.toByteData();
        };
        final Map<Integer, SchemaModel> schemaMap = this.checkTable(true,
          info, this.config.getSchemaOverrides(), schemaEncoder, schemaDecoder);

        // Check empty space before storage ID table
        this.checkEmpty(info, new KeyRange(ByteUtil.getKeyAfterPrefix(schemaTablePrefix), storageIdTablePrefix),
          "the key range between schema table and storage ID table");

        // Check the storage ID table
        final Function<ByteData, SchemaId> schemaIdDecoder = bytes -> {
            final ByteData.Reader reader = bytes.newReader();
            final SchemaId schemaId;
            try {
                schemaId = new SchemaId(Encodings.STRING.read(reader));
                if (reader.remain() > 0)
                    throw new IllegalArgumentException(String.format("trailing garbage %s", Jsck.ds(reader, reader.getOffset())));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("invalid schema ID: %s", e.getMessage()), e);
            }
            return schemaId;
        };
        final Function<SchemaId, ByteData> schemaIdEncoder = schemaId -> {
            final ByteData.Writer writer = ByteData.newWriter();
            Encodings.STRING.write(writer, schemaId.getId());
            return writer.toByteData();
        };
        final Map<Integer, SchemaId> storageIdMap = this.checkTable(false,
          info, this.config.getStorageIdOverrides(), schemaIdEncoder, schemaIdDecoder);

        // Validate schema table + storage ID table together
        info.info("validating consistency of schema and storage ID tables");
        final TreeMap<Integer, ByteData> schemaBytes = new TreeMap<>();
        final TreeMap<Integer, ByteData> storageIdBytes = new TreeMap<>();
        schemaMap.forEach((schemaIndex, schema) -> schemaBytes.put(schemaIndex, schemaEncoder.apply(schema)));
        storageIdMap.forEach((storageId, schemaId) -> storageIdBytes.put(storageId, schemaIdEncoder.apply(schemaId)));
        try {
            final SchemaBundle.Encoded encoded = new SchemaBundle.Encoded(schemaBytes, storageIdBytes);
            info.setSchemaBundle(new SchemaBundle(encoded, this.config.getEncodingRegistry()));
        } catch (InvalidSchemaException e) {
            throw new IllegalArgumentException(String.format(
              "inconsistent schema and/or storage ID tables (override(s) required): %s", e.getMessage()), e);
        }

        // Build storage objects
        info.inventoryStorages();

        // Check empty space between storage ID table and object version index
        this.checkEmpty(info, new KeyRange(ByteUtil.getKeyAfterPrefix(storageIdTablePrefix), schemaIndexKeyPrefix),
          "the key range between storage ID table and object schema index");

        // Check empty space between object schema index and user meta-data area
        this.checkEmpty(info, new KeyRange(ByteUtil.getKeyAfterPrefix(schemaIndexKeyPrefix), userMetaDataKeyPrefix),
          "the key range between object schema index and user meta-data");

        // Build map from storage ID to REPRESENTATIVE Storage instance
        final TreeMap<Integer, Storage<?>> storageMap = info.getStorages().stream()
          .collect(Collectors.toMap(Storage::getStorageId, s -> s, (s1, s2) -> s1, TreeMap::new));

        // Check empty space between storage ID ranges
        ByteData nextEmptyStartKey = ByteUtil.getKeyAfterPrefix(userMetaDataKeyPrefix);
        String prevDescription = "user meta-data";
        for (Map.Entry<Integer, Storage<?>> entry : storageMap.entrySet()) {
            final int storageId = entry.getKey();
            final  Storage<?> storage = entry.getValue();
            final ByteData stopKey = Encodings.UNSIGNED_INT.encode(storageId);
            final String nextDescription = storage.toString();
            this.checkEmpty(info, new KeyRange(nextEmptyStartKey, stopKey),
              "the key range between " + prevDescription + " and " + nextDescription);
            nextEmptyStartKey = this.getKeyRange(storageId).getMax();
            prevDescription = nextDescription;
        }

        // Check empty space after the last storage ID
        this.checkEmpty(info,
          new KeyRange(nextEmptyStartKey, ByteData.of(new byte[] { (byte)0xff })),
          "the key range after " + prevDescription);

        // Check object types
        info.getStorages().stream()
          .filter(ObjectType.class::isInstance)
          .map(ObjectType.class::cast)
          .iterator().forEachRemaining(objectType -> {
            final String rangeDescription = "the key range of " + objectType;
            final KeyRange objectTypeKeyRange = objectType.getKeyRange();
            info.info("checking %s %s", rangeDescription, objectTypeKeyRange);
            try (CloseableIterator<KVPair> ci = kv.getRange(objectTypeKeyRange)) {
                for (final PeekingIterator<KVPair> i = Iterators.peekingIterator(ci); i.hasNext(); ) {
                    final KVPair pair = i.next();
                    final ByteData idKey = pair.getKey();

                    // The next key should be an object ID
                    if (idKey.size() < ObjId.NUM_BYTES) {
                        info.handle(new InvalidKey(pair).setDetail(
                          "invalid key %s in %s: key is truncated (length %d < %d)",
                          Jsck.ds(idKey), rangeDescription, idKey.size(), ObjId.NUM_BYTES));
                        continue;
                    }
                    final ByteData.Reader idKeyReader = idKey.newReader();
                    final ObjId id = new ObjId(idKeyReader);                            // this should never throw an exception
                    assert id.getStorageId() == objectType.getStorageId();

                    // Check object meta-data
                    if (info.isDetailEnabled())
                        info.detail("checking object meta-data for %s", id);
                    int schemaIndex = -1;
                    Schema schema = null;
                    do {

                        // Check for trailing garbage after object ID, which means the object meta-data entry was missed
                        if (idKeyReader.remain() > 0) {
                            String bestGuess;
                            try {
                                final int fieldStorageId = Encodings.UNSIGNED_INT.read(idKeyReader);
                                bestGuess = "field #" + fieldStorageId;
                                final Field<?> field = objectType.schemaItem.getField(fieldStorageId);
                                bestGuess = field.toString();
                            } catch (IllegalArgumentException | UnknownFieldException e) {
                                bestGuess = "some unknown field";
                            }
                            info.handle(new InvalidKey(pair).setDetail(
                              "invalid key %s in %s: no such object %s exists (possibly orphaned content for %s)",
                              Jsck.ds(idKey), rangeDescription, id, bestGuess));
                            break;
                        }

                        // Read object schema index
                        final ByteData.Reader metaData = pair.getValue().newReader();
                        try {
                            if ((schemaIndex = Encodings.UNSIGNED_INT.read(metaData)) == 0)
                                throw new IllegalArgumentException("schema index is zero");
                        } catch (IllegalArgumentException e) {
                            info.handle(new InvalidValue(pair).setDetail(
                              "invalid meta-data %s for object %s: invalid object schema index: %s",
                              Jsck.ds(metaData), id, e.getMessage()));
                            break;
                        }

                        // Verify corresponding schema
                        try {
                            schema = info.getSchemaBundle().getSchema(schemaIndex);
                        } catch (IllegalArgumentException e) {
                            info.handle(new InvalidValue(pair).setDetail(
                              "invalid schema index %d in object %s meta-data %s: %s",
                              schemaIndex, id, Jsck.ds(metaData), e.getMessage()));
                            break;
                        }

                        // Read flags byte
                        try {
                            final int flags = metaData.remain() > 0 ? metaData.readByte() : 0;
                            if ((flags & ~Layout.OBJECT_FLAGS_VALID_BITS) != 0)
                                throw new IllegalArgumentException(String.format("invalid object flags byte 0x%02x", flags));
                            if (metaData.remain() > 0)
                                throw new IllegalArgumentException("meta-data contains trailing garbage");
                        } catch (IllegalArgumentException e) {
                            info.handle(new InvalidValue(pair, ByteData.of(new byte[] { 0 })).setDetail(
                              "invalid meta-data %s for object %s: %s", Jsck.ds(metaData), id, e.getMessage()));
                        }
                    } while (false);

                    // If object meta-data was not repairable, discard all other data in object's range
                    if (schema == null) {
                        Jsck.deleteRange(info, idKey, i, "object " + id);
                        continue;
                    }

                    // Validate object's fields content
                    if (info.isDetailEnabled())
                        info.detail("checking object content for %s", id);
                    objectType.validateObjectData(info, id, schemaIndex, i);
                }
            }
        });

        // NOTE: the checking of indexes is limited to checking "well-formedness" if we are not actually repairing the database.
        // This is because we can't see the effects of any repairs to objects that would otherwise be made, so checks for the
        // consistency of an index entry with its corresponding object is not guaranteed to be valid. By the same token, if we
        // are repairing the database, the checking of indexes must come after the checking of objects.

        // Check indexes
        final HashSet<Integer> visitedStorageIds = new HashSet<>();
        info.getStorages().stream()
          .filter(Index.class::isInstance)
          .map(Index.class::cast)
          .filter(index -> visitedStorageIds.add(index.getStorageId()))
          .iterator().forEachRemaining(index -> {
            final String rangeDescription = "the key range of " + index;
            final KeyRange indexKeyRange = index.getKeyRange();
            info.info("checking %s %s", rangeDescription, indexKeyRange);
            try (CloseableIterator<KVPair> i = kv.getRange(indexKeyRange)) {
                while (i.hasNext()) {
                    final KVPair pair = i.next();

                    // Validate index entry
                    final ByteData.Reader reader = pair.getKey().newReader();
                    try {
                        index.validateIndexEntry(info, reader);
                    } catch (IllegalArgumentException e) {
                        info.handle(new InvalidKey(pair).setDetail(index, "%s", e.getMessage()));
                        continue;
                    }

                    // Validate value, which should be empty
                    if (!pair.getValue().isEmpty())
                        info.handle(new InvalidValue(pair, ByteData.empty()).setDetail(index, "value should be empty"));
                }
            }
        });

        // Check the object version index (and detect obsolete schemas while we're at it)
        final HashSet<SchemaId> obsoleteSchemas = new HashSet<>(info.getSchemaBundle().getSchemasBySchemaId().keySet());
        final String schemaList = info.getSchemaBundle().getSchemasBySchemaIndex().entrySet().stream()
           .map(entry -> String.format("  [%d] \"%s\"", entry.getKey(), entry.getValue().getSchemaId()))
           .collect(Collectors.joining("\n"));
        final KeyRange schemaIndexKeyRange = KeyRange.forPrefix(schemaIndexKeyPrefix);
        if (!schemaList.isEmpty())
            info.info("checking object schema index %s; recorded schemas are:\n%s", schemaIndexKeyRange, schemaList);
        else
            info.info("checking object schema index %s (there are zero recorded schemas)", schemaIndexKeyRange);
        try (CloseableIterator<KVPair> i = kv.getRange(schemaIndexKeyRange)) {
            while (i.hasNext()) {
                final KVPair pair = i.next();

                // Read and validate schema index
                final ByteData.Reader reader = pair.getKey().newReader(schemaIndexKeyPrefix.size());
                final int schemaIndex;
                final Schema schema;
                try {
                    if ((schemaIndex = Encodings.UNSIGNED_INT.read(reader)) == 0)
                        throw new IllegalArgumentException("schema index is zero");
                    schema = info.getSchemaBundle().getSchema(schemaIndex);
                } catch (IllegalArgumentException e) {
                    info.handle(new InvalidKey(pair).setDetail(
                      "invalid schema index key %s: %s", Jsck.ds(reader.getByteData()), e.getMessage()));
                    continue;
                }

                // Validate index entry
                final ObjId id;
                try {

                    // Read object ID
                    try {
                        id = new ObjId(reader);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("entry contains invalid object ID", e);
                    }
                    if (reader.remain() > 0)
                        throw new IllegalArgumentException("entry contains trailing garbage");

                    // Verify object actually exists
                    if (kv.get(id.getBytes()) == null)
                        throw new IllegalArgumentException(String.format("object %s does not exist", id));
                } catch (IllegalArgumentException e) {
                    info.handle(new InvalidKey(pair).setDetail(
                      "invalid schema index value %s under schema index %d: %s",
                      Jsck.ds(reader.getByteData()), schemaIndex, e.getMessage()));
                    continue;
                }

                // Mark object's schema version in use
                if (obsoleteSchemas.remove(schema.getSchemaId()) && this.config.isGarbageCollectSchemas())
                    info.info("marking schema version \"%s\" in use", schema.getSchemaId());

                // Validate value, which should be empty
                if (!pair.getValue().isEmpty()) {
                    info.handle(new InvalidValue(pair, ByteData.empty()).setDetail(
                      "invalid schema index entry %s for \"%s\" (schema index %d): value is %s but should be empty",
                      Jsck.ds(reader.getByteData()), schema.getSchemaId(), schemaIndex, Jsck.ds(pair.getValue())));
                }
            }
        }

        // Report obsolete schema versions
        if (obsoleteSchemas.isEmpty())
            info.info("found zero obsolete schemas");
        else {
            info.info("found %s obsolete schema(s): %s", obsoleteSchemas.size(),
              obsoleteSchemas.stream()
                .map(id -> String.format("\"%s\"", id))
                .collect(Collectors.joining(", ")));
        }

        // Garbage collect obsolete schemas and storage ID's
        if (this.config.isGarbageCollectSchemas()) {

            // Build new SchemaBundle with obsolete schemas removed
            SchemaBundle schemaBundle = info.getSchemaBundle();
            SchemaBundle.Encoded encodedBundle = schemaBundle.getEncoded();
            for (SchemaId schemaId : obsoleteSchemas) {
                final Schema schema = schemaBundle.getSchema(schemaId);
                encodedBundle = schemaBundle.withSchemaRemoved(schemaId);
                schemaBundle = new SchemaBundle(encodedBundle, schemaBundle.getEncodingRegistry());
            }

            // Add fixups that remove the deleted schema table entries
            schemaBundle.getSchemasBySchemaIndex().forEach((schemaIndex, schema) -> {
                final ByteData key = Encodings.UNSIGNED_INT.encode(schemaIndex);
                final ByteData schemaVal = kv.get(key);
                if (schemaVal != null) {
                    info.handle(new InvalidValue("obsolete schema", key, schemaVal, null).setDetail(
                      "schema \"%s\"", schema.getSchemaId()), true);
                }
            });

            // Add fixups that remove the deleted storage ID table entries
            schemaBundle.getSchemaIdsByStorageId().forEach((storageId, schemaId) -> {
                final ByteData key = Encodings.UNSIGNED_INT.encode(storageId);
                final ByteData idVal = kv.get(key);
                if (idVal != null) {
                    info.handle(new InvalidValue("obsolete storage ID", key, idVal, null).setDetail(
                      "storage ID %d for schema ID \"%s\"", storageId, schemaId), true);
                }
            });
        }
    }

    private <T> Map<Integer, T> checkTable(boolean schemaTable, JsckInfo info,
      Map<Integer, T> overrides, Function<T, ByteData> encoder, Function<ByteData, T> decoder) {

        // Prep stuff
        final String tableName = schemaTable ? "schema" : "storage ID";
        final String keyName = schemaTable ? "schema index" : "storage ID";
        final String valueName = schemaTable ? "schema" : "schema ID";
        final ByteData prefix = schemaTable ? Layout.getSchemaTablePrefix() : Layout.getStorageIdTablePrefix();

        // Copy overrides so we can mutate
        overrides = overrides != null ? new HashMap<>(overrides) : Collections.emptyMap();

        // Check schema versions
        final KeyRange keyRange = KeyRange.forPrefix(prefix);
        info.info("checking the %s table %s", tableName, keyRange);
        final HashMap<Integer, T> itemMap = new HashMap<>();
        try (CloseableIterator<KVPair> i = info.getKVStore().getRange(keyRange)) {
            while (i.hasNext()) {
                final KVPair pair = i.next();
                int index = 0;
                T item = null;
                try {

                    // Decode index
                    final ByteData.Reader reader = pair.getKey().newReader(prefix.size());
                    index = Encodings.UNSIGNED_INT.read(reader);
                    if (reader.remain() > 0) {
                        throw new IllegalArgumentException(String.format(
                          "invalid %s table key %s: trailing garbage %s follows %s %d",
                          tableName, Jsck.ds(pair.getKey()), Jsck.ds(reader, reader.getOffset()), keyName, index));
                    }
                    if (index == 0) {
                        throw new IllegalArgumentException(String.format(
                          "invalid %s table key %s: invalid %s %d",
                          tableName, Jsck.ds(reader), keyName, keyName, index));
                    }

                    // Decode item
                    item = decoder.apply(pair.getValue());

                } catch (IllegalArgumentException e) {

                    // Invalid key?
                    if (index == 0) {
                        info.handle(new InvalidKey(pair).setDetail("%s", e.getMessage()));
                        continue;
                    }

                    // Replace/delete item if overridden
                    final boolean hasOverride = overrides.containsKey(index);
                    final T overrideItem = overrides.remove(index);
                    if (overrideItem != null) {
                        info.handle(new InvalidValue(pair, encoder.apply(overrideItem)).setDetail(
                          "forcibly %s invalid %s with %s %d: %s", "override", valueName, keyName, index, e.getMessage()));
                        item = overrideItem;
                    } else if (hasOverride) {
                        info.handle(new InvalidValue(pair).setDetail(
                          "forcibly %s invalid %s with %s %d: %s", "delete", valueName, keyName, index, e.getMessage()));
                        continue;
                    } else
                        throw e;
                }
                assert item != null;
                itemMap.put(index, item);
            }
        }

        // Apply any remaining overrides
        for (Map.Entry<Integer, T> entry : overrides.entrySet()) {
            final int index = entry.getKey();
            final T overrideItem = entry.getValue();
            final T originalItem = itemMap.get(index);

            // Reconstruct table key
            final ByteData.Writer writer = ByteData.newWriter(prefix.size() + 5);
            writer.write(prefix);
            Encodings.UNSIGNED_INT.write(writer, index);
            final ByteData key = writer.toByteData();

            // Apply override
            if (overrideItem == null) {
                if (originalItem != null) {
                    info.handle(new InvalidValue(key, null, null).setDetail(
                      "forcibly remove %s with %s %s", valueName, keyName, index));
                }
            } else {
                final ByteData value = encoder.apply(overrideItem);
                if (originalItem != null) {
                    info.handle(new InvalidValue(key, null, value).setDetail(
                      "forcibly %s %s with %s %d with provided version", "replace", valueName, keyName, index));
                } else {
                    info.handle(new InvalidValue(key, null, value).setDetail(
                      "forcibly %s %s with %s %d with provided version", "add", valueName, keyName, index));
                }
                itemMap.put(index, overrideItem);
            }
        }

        // Done
        return itemMap;
    }

    static void deleteRange(JsckInfo info, ByteData prefix, PeekingIterator<KVPair> i, String description) {
        while (i.hasNext() && i.peek().getKey().startsWith(prefix)) {
            final KVPair pair2 = i.next();
            assert pair2.getKey().compareTo(prefix) >= 0;
            info.handle(new InvalidKey(pair2).setDetail(
              "invalid key %s in the key range of non-existent/invalid %s", Jsck.ds(pair2.getKey()), description));
        }
    }

    static String ds(ByteData data) {
        return "[" + ParseContext.truncate(ByteUtil.toString(data), HEX_STRING_LIMIT) + "]";
    }

    static String ds(ByteData.Reader reader) {
        return Jsck.ds(reader.getByteData());
    }

    static String ds(ByteData.Reader reader, int off) {
        return Jsck.ds(reader.getByteData().substring(off));
    }

    private void checkEmpty(JsckInfo info, KeyRange range, String description) {
        if (range.isEmpty())
            return;
        info.info("checking that %s %s is empty", description, range);
        try (CloseableIterator<KVPair> i = info.getKVStore().getRange(range)) {
            while (i.hasNext()) {
                final KVPair pair = i.next();
                info.handle(new InvalidKey(pair.getKey(), pair.getValue()).setDetail("%s", description));
            }
        }
    }

    private KeyRange getKeyRange(int storageId) {
        return KeyRange.forPrefix(Encodings.UNSIGNED_INT.encode(storageId));
    }
}
