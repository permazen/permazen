
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.jsck;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;

import org.jsimpledb.core.Database;
import org.jsimpledb.core.InvalidSchemaException;
import org.jsimpledb.core.Layout;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.schema.SchemaObjectType;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.CloseableIterator;
import org.jsimpledb.util.ParseContext;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Applies consistency checks to, and optionally repairs corruption of, a JSimpleDB key/value database.
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
     * @throws IllegalArgumentException if database is not a JSimpleDB database
     *  (i.e., no {@linkplain Layout#getFormatVersionKey format version key})
     * @throws IllegalArgumentException if a {@linkplain JsckConfig#getForceSchemaVersions forced schema version} is invalid
     * @throws IllegalArgumentException if the database's recorded schemas are mutually inconsistent
     *  (requiring forced schema version overrides)
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
        final byte[] formatVersionKey = Layout.getFormatVersionKey();
        final byte[] schemaKeyPrefix = Layout.getSchemaKeyPrefix();
        final byte[] objectVersionIndexKeyPrefix = Layout.getObjectVersionIndexKeyPrefix();
        final byte[] userMetaDataKeyPrefix = Layout.getUserMetaDataKeyPrefix();

        assert schemaKeyPrefix[0] == 0;
        assert objectVersionIndexKeyPrefix[0] == 0;
        assert userMetaDataKeyPrefix[0] == 0;

        assert ByteUtil.compare(schemaKeyPrefix, formatVersionKey) > 0;
        assert ByteUtil.compare(objectVersionIndexKeyPrefix, schemaKeyPrefix) > 0;
        assert ByteUtil.compare(userMetaDataKeyPrefix, objectVersionIndexKeyPrefix) > 0;

        // Check format version
        info.info("checking format version");
        int forceFormatVersion = this.config.getForceFormatVersion();
        if (forceFormatVersion < 0 || forceFormatVersion > Layout.CURRENT_FORMAT_VERSION)
            forceFormatVersion = 0;
        final KVStore kv = info.getKVStore();
        byte[] val = kv.get(formatVersionKey);
        try {
            if (val == null) {
                throw new IllegalArgumentException(kv.getAtLeast(ByteUtil.EMPTY, null) == null ? "database is empty" :
                  "missing JSimpleDB signature/format version key " + Jsck.ds(formatVersionKey));
            }
            final ByteReader reader = new ByteReader(val);
            try {
                info.setFormatVersion(UnsignedIntEncoder.read(reader));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("invalid JSimpleDB signature/format version value "
                  + Jsck.ds(val) + ": can't decode version number");
            }
            if (reader.remain() > 0) {
                throw new IllegalArgumentException("invalid JSimpleDB signature/format version value "
                  + Jsck.ds(val) + ": trailing garbage " + Jsck.ds(reader, reader.getOffset())
                  + " follows format version number " + info.getFormatVersion());
            }
            switch (info.getFormatVersion()) {
            case Layout.FORMAT_VERSION_1:
            case Layout.FORMAT_VERSION_2:
                break;
            default:
                throw new IllegalArgumentException("invalid JSimpleDB signature/format version key value "
                  + Jsck.ds(val) + ": unrecognized format version number " + info.getFormatVersion());
            }
        } catch (IllegalArgumentException e) {
            if (forceFormatVersion == 0)
                throw e;
            info.setFormatVersion(forceFormatVersion);
            final byte[] newValue = UnsignedIntEncoder.encode(info.getFormatVersion());
            info.handle(new InvalidValue(formatVersionKey, val, newValue).setDetail(e.getMessage()));
        }

        // Check empty space before format version
        this.checkEmpty(info, new KeyRange(ByteUtil.EMPTY, Layout.getFormatVersionKey()), "key range prior to format version");

        // Check empty space before schemas
        this.checkEmpty(info, new KeyRange(ByteUtil.getNextKey(Layout.getFormatVersionKey()), schemaKeyPrefix),
          "key range between format version and recorded schemas");

        // Check schema versions
        info.info("checking recorded schema versions");
        Map<Integer, SchemaModel> forceSchemaVersions = this.config.getForceSchemaVersions();
        if (forceSchemaVersions == null)
            forceSchemaVersions = Collections.emptyMap();
        try (final CloseableIterator<KVPair> i = kv.getRange(Layout.getSchemaKeyRange())) {
            while (i.hasNext()) {
                final KVPair pair = i.next();
                int version = 0;
                SchemaModel schema = null;
                try {

                    // Get version number
                    final ByteReader reader = new ByteReader(pair.getKey());
                    reader.skip(schemaKeyPrefix.length);
                    version = UnsignedIntEncoder.read(reader);
                    if (reader.remain() > 0) {
                        throw new IllegalArgumentException("invalid schema entry key " + Jsck.ds(pair.getKey())
                          + ": trailing garbage " + Jsck.ds(reader, reader.getOffset())
                          + " follows format version number " + info.getFormatVersion());
                    }
                    if (version == 0) {
                        throw new IllegalArgumentException("invalid schema entry key "
                          + Jsck.ds(val) + ": version number is zero");
                    }

                    // Decode schema model
                    try {
                        schema = Layout.decodeSchema(pair.getValue(), info.getFormatVersion());
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("invalid encoded schema: " + e.getMessage(), e);
                    } catch (InvalidSchemaException e) {
                        throw new IllegalArgumentException("invalid schema model: " + e.getMessage(), e);
                    }

                    // Is there a forced override? If so, replace/delete schema version
                    if (forceSchemaVersions.containsKey(version)) {
                        final SchemaModel forcedSchema = forceSchemaVersions.get(version);
                        if (!schema.equals(forcedSchema)) {
                            if (forcedSchema != null) {
                                final byte[] newValue = Layout.encodeSchema(forcedSchema, info.getFormatVersion());
                                info.handle(new InvalidValue(pair, newValue).setDetail("forcibly override schema version "
                                  + version + " with provided version having these differences: "
                                  + forcedSchema.differencesFrom(schema)));
                                schema = forcedSchema;
                            } else {
                                info.handle(new InvalidValue(pair).setDetail("forcibly delete schema version " + version));
                                continue;
                            }
                        }
                    }
                } catch (IllegalArgumentException e) {

                    // Invalid version/key?
                    if (version == 0) {
                        info.handle(new InvalidKey(pair).setDetail(e.getMessage()));
                        continue;
                    }

                    // Is there no forced override? If so, bail out requiring user to correct
                    if (!forceSchemaVersions.containsKey(version)) {
                        throw new IllegalArgumentException("schema version " + version
                          + " is invalid (forced schema override required): " + e.getMessage(), e);
                    }

                    // Replace/delete schema version with forced override
                    final SchemaModel forcedSchema = forceSchemaVersions.get(version);
                    if (forcedSchema != null) {
                        info.handle(new InvalidValue(pair, Layout.encodeSchema(forcedSchema, info.getFormatVersion())).setDetail(
                          "forcibly override invalid schema version " + version + " which is invalid anyway: " + e.getMessage()));
                        schema = forcedSchema;
                    } else {
                        info.handle(new InvalidValue(pair).setDetail("forcibly delete schema version "
                          + version + " which is invalid anyway: " + e.getMessage()));
                        continue;
                    }
                }
                assert schema != null;

                // Validate schema, and if not valid, bail out requiring user to correct
                info.info("validating schema version " + version);
                try {
                    Database.validateSchema(this.config.getFieldTypeRegistry(), schema);
                } catch (InvalidSchemaException e) {
                    throw new IllegalArgumentException((schema == forceSchemaVersions.get(version) ? "forced " : "")
                      + "schema version " + version + " is invalid: " + e.getMessage(), e);
                }

                // Note recorded schema
                info.getSchemas().put(version, schema);
            }
        }

        // Install any forced schema versions that do not exist in the database
        for (Map.Entry<Integer, SchemaModel> entry : forceSchemaVersions.entrySet()) {
            final int version = entry.getKey();
            final SchemaModel schema = entry.getValue();
            if (info.getSchemas().containsKey(version))
                continue;
            try {
                Database.validateSchema(this.config.getFieldTypeRegistry(), schema);
            } catch (InvalidSchemaException e) {
                throw new IllegalArgumentException("forced schema version " + version + " is invalid: " + e.getMessage(), e);
            }
            final ByteWriter writer = new ByteWriter(schemaKeyPrefix.length + 5);
            writer.write(schemaKeyPrefix);
            UnsignedIntEncoder.write(writer, version);
            final byte[] newKey = writer.getBytes();
            final byte[] newValue = Layout.encodeSchema(schema, info.getFormatVersion());
            info.handle(new InvalidValue(newKey, null, newValue).setDetail(
              "forcibly override schema version " + version + " with provided version"));
        }

        // Validate schemas are mutually consistent; if not, bail out requiring user to correct
        info.info("validating schema compatibility among all versions " + info.getSchemas().keySet());
        try {
            Database.validateSchemas(this.config.getFieldTypeRegistry(), info.getSchemas().values());
        } catch (InvalidSchemaException e) {
            throw new IllegalArgumentException("database schemas are mutually inconsistent (forced schema override(s) required): "
              + e.getMessage(), e);
        }

        // Build lookup maps
        info.inventoryStorages();

        // Check empty space between schemas and object version index
        this.checkEmpty(info, new KeyRange(ByteUtil.getKeyAfterPrefix(schemaKeyPrefix), objectVersionIndexKeyPrefix),
          "key range between recorded schemas and object version index");

        // Check empty space between object version index and user meta-data area
        this.checkEmpty(info,
          new KeyRange(ByteUtil.getKeyAfterPrefix(objectVersionIndexKeyPrefix), Layout.getUserMetaDataKeyPrefix()),
          "key range between object version index and user meta-data");

        // Get all storage ID's
        final int[] allStorageIds = info.getStorages().values().stream()
          .map(Map::keySet)
          .flatMapToInt(keys -> keys.stream().mapToInt(Integer::intValue))
          .sorted()
          .distinct()
          .toArray();

        // Check empty space between storage ID ranges
        byte[] nextEmptyStartKey = ByteUtil.getKeyAfterPrefix(Layout.getUserMetaDataKeyPrefix());
        String prevDescription = "user meta-data";
        for (int storageId : allStorageIds) {
            final byte[] stopKey = UnsignedIntEncoder.encode(storageId);
            final String nextDescription = "storage ID " + storageId;
            this.checkEmpty(info, new KeyRange(nextEmptyStartKey, stopKey),
              "key range between " + prevDescription + " and " + nextDescription);
            nextEmptyStartKey = this.getKeyRange(storageId).getMax();
            prevDescription = nextDescription;
        }

        // Check empty space after the last storage ID
        this.checkEmpty(info, new KeyRange(nextEmptyStartKey, new byte[] { (byte)0xff }), "key range after " + prevDescription);

        // Get all object type storage ID's
        final int[] objectTypeStorageIds = info.getStorages().values().stream()
          .map(Map::values)
          .flatMap(Collection::stream)
          .filter(ObjectType.class::isInstance)
          .mapToInt(Storage::getStorageId)
          .sorted()
          .distinct()
          .toArray();

        // Check object types
        for (int storageId : objectTypeStorageIds) {
            final String rangeDescription = "the key range of object type storage ID " + storageId;
            info.info("checking " + rangeDescription);
            try (final CloseableIterator<KVPair> ci = kv.getRange(this.getKeyRange(storageId))) {
                for (final PeekingIterator<KVPair> i = Iterators.peekingIterator(ci); i.hasNext(); ) {
                    final KVPair pair = i.next();
                    final byte[] idKey = pair.getKey();

                    // The next key should be an object ID
                    if (idKey.length < ObjId.NUM_BYTES) {
                        info.handle(new InvalidKey(pair).setDetail("invalid key " + Jsck.ds(idKey) + " in " + rangeDescription
                          + ": key is truncated (length " + idKey.length + " < " + ObjId.NUM_BYTES + ")"));
                        continue;
                    }
                    final ByteReader idKeyReader = new ByteReader(idKey);
                    final ObjId id = new ObjId(idKeyReader);                            // this should never throw an exception

                    // Check object meta-data
                    if (info.isDetailEnabled())
                        info.detail("checking object meta-data for " + id);
                    SchemaObjectType objectType = null;
                    int version = 0;
                    do {

                        // Check for extra garbage after object ID
                        if (idKeyReader.remain() > 0) {
                            String detail = "invalid key " + Jsck.ds(idKey) + " in " + rangeDescription
                              + ": no such object " + id + " exists";
                            int fieldStorageId = -1;
                            try {
                                fieldStorageId = UnsignedIntEncoder.read(idKeyReader);
                            } catch (IllegalArgumentException e) {
                                // ignore
                            }
                            if (fieldStorageId > 0 && idKeyReader.remain() == 0)
                                detail += " (possibly orphaned content for field #" + fieldStorageId + ")";
                            info.handle(new InvalidKey(pair).setDetail(detail));
                            break;
                        }

                        // Read meta-data format version
                        final ByteReader metaData = new ByteReader(pair.getValue());
                        try {
                            final int metaDataVersion = UnsignedIntEncoder.read(metaData);
                            switch (metaDataVersion) {
                            case 1:
                                break;
                            case 0:
                                throw new IllegalArgumentException("invalid zero object meta-data format version");
                            default:
                                throw new IllegalArgumentException("unknown object meta-data format version " + metaDataVersion);
                            }
                        } catch (IllegalArgumentException e) {
                            info.handle(new InvalidValue(pair).setDetail("invalid meta-data " + Jsck.ds(metaData)
                              + " for object " + id + ": can't decode object meta-data format version: " + e.getMessage()));
                            break;
                        }

                        // Read object schema version
                        try {
                            if ((version = UnsignedIntEncoder.read(metaData)) == 0)
                                throw new IllegalArgumentException("invalid zero version number");
                        } catch (IllegalArgumentException e) {
                            info.handle(new InvalidValue(pair).setDetail("invalid meta-data " + Jsck.ds(metaData)
                              + " for object " + id + ": can't decode object schema version: " + e.getMessage()));
                            break;
                        }

                        // Retrieve schema
                        final SchemaModel schema = info.getSchemas().get(version);
                        if (schema == null) {
                            info.handle(new InvalidValue(pair).setDetail("invalid meta-data "
                              + Jsck.ds(metaData) + " for object " + id + ": invalid schema version " + version
                              + ": no such schema version exists"));
                            break;
                        }

                        // Retrieve schema object type
                        if ((objectType = schema.getSchemaObjectTypes().get(id.getStorageId())) == null) {
                            info.handle(new InvalidValue(pair).setDetail("invalid object ID " + id
                              + " with storage ID " + id.getStorageId() + ": no such object type exists in schema version "
                              + version));
                            break;
                        }

                        // Read delete notified flag - since it should always be zero, we can always fix it
                        final int mark = metaData.mark();
                        try {
                            if (metaData.remain() == 0)
                                throw new IllegalArgumentException("missing delete notified byte");
                            final int deleteNotified = metaData.readByte();
                            if (deleteNotified != 0) {
                                throw new IllegalArgumentException(String.format(
                                  "invalid notified byte 0x%02x != 0x00", deleteNotified));
                            }
                            if (metaData.remain() > 0)
                                throw new IllegalArgumentException("meta-data contains extra garbage");
                        } catch (IllegalArgumentException e) {
                            final ByteWriter fixup = new ByteWriter(mark + 1);
                            fixup.write(metaData.getBytes(0, mark));
                            fixup.writeByte(0);
                            info.handle(new InvalidValue(pair, fixup.getBytes()).setDetail("invalid meta-data "
                              + Jsck.ds(metaData) + " for object " + id + ": " + e.getMessage()));
                        }
                    } while (false);

                    // If object meta-data was not repairable, discard all other data in object's range
                    if (objectType == null) {
                        Jsck.deleteRange(info, idKey, i, "object " + id);
                        continue;
                    }

                    // Find corresponding object type storage
                    final ObjectType objType = (ObjectType)info.getStorages().get(version).get(id.getStorageId());
                    assert objType != null;

                    // Validate object's fields content
                    assert version > 0;
                    if (info.isDetailEnabled())
                        info.detail("checking object content for " + id);
                    objType.validateObjectData(info, id, version, i);
                }
            }
        }

        // Get all index storage ID's
        final int[] indexStorageIds = info.getStorages().values().stream()
          .map(Map::values)
          .flatMap(Collection::stream)
          .filter(Index.class::isInstance)
          .mapToInt(Storage::getStorageId)
          .sorted()
          .distinct()
          .toArray();

        // NOTE: the checking of indexes is limited to checking "well-formedness" if we are not actually repairing the database.
        // This is because we can't see the effects of any repairs to objects that would otherwise be made, so checks for the
        // consistency of an index entry with its corresponding object is not guaranteed to be valid. By the same token, if we
        // are repairing the database, the checking of indexes must come after the checking of objects.

        // Check indexes
        for (int storageId : indexStorageIds) {
            final Index index = info.getIndexes().get(storageId);
            final String rangeDescription = "the key range of " + index;
            info.info("checking " + rangeDescription);
            try (final CloseableIterator<KVPair> i = kv.getRange(index.getKeyRange())) {
                while (i.hasNext()) {
                    final KVPair pair = i.next();

                    // Validate index entry
                    final ByteReader reader = new ByteReader(pair.getKey());
                    try {
                        index.validateIndexEntry(info, reader);
                    } catch (IllegalArgumentException e) {
                        info.handle(new InvalidKey(pair).setDetail(index, e.getMessage()));
                        continue;
                    }

                    // Validate value, which should be empty
                    if (pair.getValue().length > 0)
                        info.handle(new InvalidValue(pair, ByteUtil.EMPTY).setDetail(index, "value should be empty"));
                }
            }
        }

        // Check the object version index
        info.info("checking object version index");
        final HashSet<Integer> unusedSchemaVersions = new HashSet<>(info.getSchemas().keySet());
        try (final CloseableIterator<KVPair> i = kv.getRange(KeyRange.forPrefix(objectVersionIndexKeyPrefix))) {
            while (i.hasNext()) {
                final KVPair pair = i.next();

                // Read version
                final ByteReader reader = new ByteReader(pair.getKey());
                reader.skip(objectVersionIndexKeyPrefix.length);
                final int version;
                try {
                    version = UnsignedIntEncoder.read(reader);
                } catch (IllegalArgumentException e) {
                    info.handle(new InvalidKey(pair).setDetail("invalid object version index entry "
                      + Jsck.ds(reader.getBytes()) + ": can't interpret version number: " + e.getMessage()));
                    continue;
                }

                // Read object ID
                final ObjId id;
                try {
                    id = new ObjId(reader);
                    if (reader.remain() > 0)
                        throw new IllegalArgumentException("index entry contains extra garbage");
                } catch (IllegalArgumentException e) {
                    info.handle(new InvalidKey(pair).setDetail("invalid object version index entry "
                      + Jsck.ds(reader.getBytes()) + " for version " + version + ": can't interpret object ID: "
                      + e.getMessage()));
                    continue;
                }

                // Verify object still exists
                if (kv.get(id.getBytes()) == null) {
                    info.handle(new InvalidKey(pair).setDetail("invalid object version index entry "
                      + Jsck.ds(reader.getBytes()) + " for version " + version + ": object " + id + " does not exist"));
                    continue;
                }

                // Mark object's schema version in use
                unusedSchemaVersions.remove(version);

                // Validate value, which should be empty
                if (pair.getValue().length > 0) {
                    info.handle(new InvalidValue(pair, ByteUtil.EMPTY).setDetail("invalid object version index entry "
                      + Jsck.ds(reader.getBytes()) + " for version " + version + ": value is " + Jsck.ds(pair.getValue())
                      + " but should be empty"));
                }
            }
        }

        // Garbage collect schema versions
        if (this.config.isGarbageCollectSchemas()) {
            info.info("checking object version index");
            for (int version : unusedSchemaVersions) {
                final byte[] key = Layout.getSchemaKey(version);
                final byte[] value = kv.get(key);
                if (value != null)
                    info.handle(new InvalidKey(key, value).setDetail("unused schema version " + version));
            }
        }
    }

    static void deleteRange(JsckInfo info, byte[] prefix, PeekingIterator<KVPair> i, String description) {
        while (i.hasNext() && ByteUtil.isPrefixOf(prefix, i.peek().getKey())) {
            final KVPair pair2 = i.next();
            assert ByteUtil.compare(pair2.getKey(), prefix) >= 0;
            info.handle(new InvalidKey(pair2).setDetail("invalid key " + Jsck.ds(pair2.getKey())
              + " in the key range of non-existent/invalid " + description));
        }
    }

    static String ds(byte[] data) {
        return "[" + ParseContext.truncate(ByteUtil.toString(data), HEX_STRING_LIMIT) + "]";
    }

    static String ds(ByteReader reader) {
        return Jsck.ds(reader.getBytes());
    }

    static String ds(ByteReader reader, int off) {
        return Jsck.ds(reader.getBytes(off));
    }

    private long checkEmpty(JsckInfo info, KeyRange range, String description) {
        info.info("checking that " + description + " is empty");
        long count = 0;
        try (final CloseableIterator<KVPair> i = info.getKVStore().getRange(range)) {
            while (i.hasNext()) {
                final KVPair pair = i.next();
                info.handle(new InvalidKey(pair.getKey(), pair.getValue()).setDetail(description));
            }
        }
        return count;
    }

    private KeyRange getKeyRange(int storageId) {
        return KeyRange.forPrefix(UnsignedIntEncoder.encode(storageId));
    }
}

