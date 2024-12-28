
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.schema.SchemaModel;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.UnsignedIntEncoder;

import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Constants and utility methods relating to the encoding and layout of a {@link Database} in a key/value store.
 *
 * <p>
 * The key/value space is divided into a data area and a meta-data area. The data area contains object data as
 * well as simple and composite index data. The meta-data area contains a recognizable signature, database format version,
 * all recorded schemas, the object schema index, and a range reserved for user applications.
 *
 * @see <a href="https://github.com/permazen/permazen/blob/master/LAYOUT.md">LAYOUT.md</a>
 */
public final class Layout {

    /**
     * The original {@link Database} layout format version #1.
     */
    public static final int FORMAT_VERSION_1 = 1;                                       // original format

    /**
     * The current {@link Database} layout format version ({@value #CURRENT_FORMAT_VERSION}).
     */
    public static final int CURRENT_FORMAT_VERSION = FORMAT_VERSION_1;

    /**
     * The single byte value that is a prefix of all meta-data keys.
     */
    public static final int METADATA_PREFIX_BYTE = 0x00;

    /**
     * The single byte that follows {@link #METADATA_PREFIX_BYTE} to form the format version key.
     */
    public static final int METADATA_FORMAT_VERSION_BYTE = 0x00;

    /**
     * The single byte that follows {@link #METADATA_PREFIX_BYTE} to indicate the schema table.
     */
    public static final int METADATA_SCHEMA_TABLE_BYTE = 0x01;

    /**
     * The single byte that follows {@link #METADATA_PREFIX_BYTE} to indicate the storage ID table.
     */
    public static final int METADATA_STORAGE_ID_TABLE_BYTE = 0x02;

    /**
     * The single byte that follows {@link #METADATA_PREFIX_BYTE} to indicate the object schema index.
     */
    public static final int METADATA_SCHEMA_INDEX_BYTE = 0x80;

    /**
     * The single byte that follows {@link #METADATA_PREFIX_BYTE} to indicate the user meta-data area.
     */
    public static final int METADATA_USER_META_DATA_BYTE = 0xff;

    /**
     * Object meta-data flags byte valid bits.
     *
     * <p>
     * All bits must be zero.
     */
    public static final int OBJECT_FLAGS_VALID_BITS = 0x00;

    private static final ByteData METADATA_PREFIX = ByteData.of(METADATA_PREFIX_BYTE);

    private static final ByteData FORMAT_VERSION_KEY = ByteData.of(
      METADATA_PREFIX_BYTE, METADATA_FORMAT_VERSION_BYTE, 'P', 'e', 'r', 'm', 'a', 'z', 'e', 'n', 0x00);

    private static final ByteData SCHEMA_TABLE_PREFIX = ByteData.of(
      METADATA_PREFIX_BYTE, METADATA_SCHEMA_TABLE_BYTE);

    // This must immediately follow SCHEMA_TABLE_PREFIX
    private static final ByteData STORAGE_ID_TABLE_PREFIX = ByteData.of(
      METADATA_PREFIX_BYTE, METADATA_STORAGE_ID_TABLE_BYTE);

    private static final ByteData SCHEMA_INDEX_PREFIX = ByteData.of(
      METADATA_PREFIX_BYTE, METADATA_SCHEMA_INDEX_BYTE);

    private static final ByteData USER_META_DATA_KEY_PREFIX = ByteData.of(
      METADATA_PREFIX_BYTE, METADATA_USER_META_DATA_BYTE);

    // Sanity check assumptions
    static {
        final ByteData schemaPrefix = Layout.getSchemaTablePrefix();
        final ByteData storageIdPrefix = Layout.getStorageIdTablePrefix();
        Preconditions.checkState(schemaPrefix.size() == storageIdPrefix.size());
        Preconditions.checkState(storageIdPrefix.equals(ByteUtil.getKeyAfterPrefix(schemaPrefix)));
    }

    private Layout() {
    }

    /**
     * Get the common prefix of all meta-data keys.
     *
     * @return meta-data prefix bytes
     */
    public static ByteData getMetaDataKeyPrefix() {
        return METADATA_PREFIX;
    }

    /**
     * Get the key under which the database format version is encoded.
     *
     * <p>
     * The existence of this key also serves to identify a Permazen database.
     *
     * @return meta-data prefix bytes
     */
    public static ByteData getFormatVersionKey() {
        return FORMAT_VERSION_KEY;
    }

    /**
     * Get the common prefix of all schema table keys.
     *
     * @return schema table key prefix
     */
    public static ByteData getSchemaTablePrefix() {
        return SCHEMA_TABLE_PREFIX;
    }

    /**
     * Get the common prefix of all storage ID table keys.
     *
     * @return storage ID table key prefix
     */
    public static ByteData getStorageIdTablePrefix() {
        return STORAGE_ID_TABLE_PREFIX;
    }

    /**
     * Get the key corresponding to an entry in an indexed table (e.g., schema table or storage ID table).
     *
     * @param prefix table key range prefix
     * @param index table index
     * @return key/value store key
     * @throws IllegalArgumentException if {@code index} is zero or negative
     * @throws IllegalArgumentException if {@code prefix} is null
     */
    public static ByteData buildTableKey(ByteData prefix, int index) {
        Preconditions.checkArgument(prefix != null, "null prefix");
        Preconditions.checkArgument(index > 0, "index <= 0");
        final ByteData.Writer writer = ByteData.newWriter(prefix.size() + UnsignedIntEncoder.encodeLength(index));
        writer.write(prefix);
        UnsignedIntEncoder.write(writer, index);
        return writer.toByteData();
    }

    /**
     * Get the common prefix of all object schema index entries.
     *
     * @return object schema index key prefix
     */
    public static ByteData getSchemaIndexKeyPrefix() {
        return SCHEMA_INDEX_PREFIX;
    }

    /**
     * Get the common prefix of all user-defined meta-data keys.
     *
     * @return user meta-data key prefix
     */
    public static ByteData getUserMetaDataKeyPrefix() {
        return USER_META_DATA_KEY_PREFIX;
    }

    /**
     * Get a {@link CoreIndex1} view of the object schema index in a key/value database.
     *
     * @param kv key/value data
     * @return object schema index
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public static CoreIndex1<Integer, ObjId> getSchemaIndex(KVStore kv) {
        Preconditions.checkArgument(kv != null, "null kv");
        return new CoreIndex1<>(kv, new Index1View<>(SCHEMA_INDEX_PREFIX, false, Encodings.UNSIGNED_INT, Encodings.OBJ_ID));
    }

    /**
     * Build the key for an object schema index entry.
     *
     * @param id object ID
     * @param schemaIndex object schema index
     * @return schemaIndex index entry key
     * @throws IllegalArgumentException if {@code id} is null
     * @throws IllegalArgumentException if {@code schemaIndex} is non-positive
     */
    public static ByteData buildSchemaIndexKey(ObjId id, int schemaIndex) {
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(schemaIndex > 0, "non-positive schemaIndex");
        final ByteData.Writer writer = ByteData.newWriter(SCHEMA_INDEX_PREFIX.size()
          + UnsignedIntEncoder.encodeLength(schemaIndex) + ObjId.NUM_BYTES);
        writer.write(SCHEMA_INDEX_PREFIX);
        UnsignedIntEncoder.write(writer, schemaIndex);
        id.writeTo(writer);
        return writer.toByteData();
    }

    /**
     * Decode schema XML from a schema table entry.
     *
     * @param schemaData compressed XML input
     * @return decoded schema model
     * @throws InvalidSchemaException if data or schema is invalid
     * @throws IllegalArgumentException if {@code schemaData} is null
     */
    public static SchemaModel decodeSchema(ByteData schemaData) {

        // Sanity check
        Preconditions.checkArgument(schemaData != null, "null schemaData");

        // Decompress and decode XML
        try (
          ByteData.Reader reader = schemaData.newReader();
          InflaterInputStream input = new InflaterInputStream(reader)) {
            final SchemaModel schemaModel = SchemaModel.fromXML(input);
            if (reader.remain() > 0)
                throw new InvalidSchemaException("compressed schema XML contains trailing garbage");
            return schemaModel;
        } catch (IOException e) {
            throw new InvalidSchemaException(String.format("error in compressed schema XML data: %s", e.getMessage()), e);
        }
    }

    /**
     * Encode schema XML for a schema table entry.
     *
     * @param writer compressed XML output
     * @param schemaModel schema model
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void encodeSchema(ByteData.Writer writer, SchemaModel schemaModel) {

        // Sanity check
        Preconditions.checkArgument(writer != null, "null writer");
        Preconditions.checkArgument(schemaModel != null, "null schemaModel");

        // Encode and compress XML
        final Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
        try (DeflaterOutputStream output = new DeflaterOutputStream(writer, deflater)) {
            schemaModel.toXML(output, true, false);
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }
    }

    /**
     * Delete all object and index data from the given {@link KVStore}.
     *
     * <p>
     * Upon return, the {@link KVStore} will still contain meta-data, but not any objects.
     *
     * @param kv key/value database
     */
    public static void deleteObjectData(KVStore kv) {

        // Get ranges
        final KeyRange metaDataRange = KeyRange.forPrefix(Layout.getMetaDataKeyPrefix());
        final KeyRange schemaIndexRange = KeyRange.forPrefix(Layout.getSchemaIndexKeyPrefix());
        assert metaDataRange.contains(schemaIndexRange);

        // Delete everything except meta-data
        kv.removeRange(null, metaDataRange.getMin());
        kv.removeRange(metaDataRange.getMax(), null);

        // Delete the object schema index
        kv.removeRange(metaDataRange.getMin(), schemaIndexRange.getMin());
        kv.removeRange(schemaIndexRange.getMax(), metaDataRange.getMax());
    }

    /**
     * Copy non-object meta-data from one {@link KVStore} to another.
     *
     * <p>
     * This copies all meta-data except the object schema index. Any existing key/value pairs
     * in the destination meta-data range are not removed prior to the copy.
     *
     * @param src source key/value database
     * @param dst destination key/value database
     */
    public static void copyMetaData(KVStore src, KVStore dst) {

        // Get ranges
        final KeyRange metaDataRange = KeyRange.forPrefix(Layout.getMetaDataKeyPrefix());
        final KeyRange schemaIndexRange = KeyRange.forPrefix(Layout.getSchemaIndexKeyPrefix());
        assert metaDataRange.contains(schemaIndexRange);

        // Copy meta-data
        Layout.copyRange(src, dst, metaDataRange.getMin(), schemaIndexRange.getMin());
        Layout.copyRange(src, dst, schemaIndexRange.getMax(), metaDataRange.getMax());
    }

    private static void copyRange(KVStore src, KVStore dst, ByteData minKey, ByteData maxKey) {
        try (CloseableIterator<KVPair> i = src.getRange(minKey, maxKey)) {
            while (i.hasNext()) {
                final KVPair pair = i.next();
                dst.put(pair.getKey(), pair.getValue());
            }
        }
    }
}
