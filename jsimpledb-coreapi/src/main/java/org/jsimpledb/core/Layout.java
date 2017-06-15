
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.CloseableIterator;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * Constants and utility methods relating to the encoding and layout of a {@link Database} in a key/value store.
 *
 * <p>
 * The key/value space is divided into a data area and a meta-data area. The data area contains object data as
 * well as simple and composite index data. The meta-data area contains a recognizable signature, encoding format version,
 * each recorded schema version, the object version index data, and a range reserved for user applications.
 *
 * @see <a href="https://github.com/archiecobbs/jsimpledb/blob/master/LAYOUT.txt">LAYOUT.txt</a>
 */
public final class Layout {

    /**
     * The original {@link Database} layout format version #1.
     */
    public static final int FORMAT_VERSION_1 = 1;                                       // original format

    /**
     * {@link Database} layout format version #2.
     *
     * <p>
     * This version switched to compressed schema XML.
     */
    public static final int FORMAT_VERSION_2 = 2;                                       // added compressed schema XML

    /**
     * The current {@link Database} layout format version ({@value #CURRENT_FORMAT_VERSION}).
     */
    public static final int CURRENT_FORMAT_VERSION = FORMAT_VERSION_2;

    /**
     * The single byte value that is a prefix of all meta-data keys.
     */
    private static final byte METADATA_PREFIX_BYTE = (byte)0x00;

    private static final byte[] METADATA_PREFIX = new byte[] { METADATA_PREFIX_BYTE };

    private static final byte[] FORMAT_VERSION_KEY = new byte[] {
      METADATA_PREFIX_BYTE, (byte)0x00,
      (byte)'J', (byte)'S', (byte)'i', (byte)'m', (byte)'p', (byte)'l', (byte)'e', (byte)'D', (byte)'B'
    };

    private static final byte[] SCHEMA_KEY_PREFIX = new byte[] {
      METADATA_PREFIX_BYTE, (byte)0x01
    };
    private static final byte[] VERSION_INDEX_PREFIX = new byte[] {
      METADATA_PREFIX_BYTE, (byte)0x80
    };
    private static final byte[] USER_META_DATA_KEY_PREFIX = new byte[] {
      METADATA_PREFIX_BYTE, (byte)0xff
    };

    // Note: this string must not ever change
    private static final byte[] SCHEMA_XML_COMPRESSION_DICTIONARY = (""
      + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
      + "<Schema formatVersion=\"2"
      + "<CompositeIndex"
      + "<Counter"
      + "<Enum"
      + "<Identifier"
      + "<Indexed"
      + "<List"
      + "<Map"
      + "<ObjectType"
      + "<ReferenceField"
      + "<SetField"
      + "<SimpleField storageId=\""
      + " cascadeDelete=\""
      + " encodingSignature=\""
      + " indexed=\""
      + " name=\""
      + " onDelete=\""
      + " type=\""
      + "boolean"
      + "byte[]"
      + "short"
      + "char"
      + "integer"
      + "float"
      + "long"
      + "double"
      + "java.lang.String"
      + "util.Date"
      + "UUID"
      + "URI"
      + "io.File"
      + "regex.Pattern"
      + "true"
      + "false"
      + "NOTHING"
      + "EXCEPTION"
      + "UNREFERENCE"
      + "DELETE"
      + "\"><")
      .getBytes(StandardCharsets.UTF_8);

    private Layout() {
    }

    /**
     * Get the common prefix of all meta-data keys.
     *
     * @return meta-data prefix bytes
     */
    public static byte[] getMetaDataKeyPrefix() {
        return METADATA_PREFIX.clone();
    }

    /**
     * Get the meta-data key range.
     *
     * @return meta-data key range
     */
    public static KeyRange getMetaDataKeyRange() {
        return KeyRange.forPrefix(METADATA_PREFIX);
    }

    /**
     * Get the key under which the database format version is encoded.
     *
     * <p>
     * The existence of this key also serves to identify a JSimpleDB database.
     *
     * @return meta-data prefix bytes
     */
    public static byte[] getFormatVersionKey() {
        return FORMAT_VERSION_KEY.clone();
    }

    /**
     * Get the common prefix of all schema version keys.
     *
     * @return schema version key prefix
     */
    public static byte[] getSchemaKeyPrefix() {
        return SCHEMA_KEY_PREFIX.clone();
    }

    /**
     * Get the schema key range.
     *
     * @return schema key range
     */
    public static KeyRange getSchemaKeyRange() {
        return KeyRange.forPrefix(SCHEMA_KEY_PREFIX);
    }

    /**
     * Get the key corresponding to the specified schema version.
     *
     * @param version schema version
     * @return schema key
     * @throws IllegalArgumentException if {@code version} is zero or negative
     */
    public static byte[] buildSchemaKey(int version) {
        Preconditions.checkArgument(version > 0, "version <= 0");
        final ByteWriter writer = new ByteWriter(VERSION_INDEX_PREFIX.length + UnsignedIntEncoder.encodeLength(version));
        writer.write(SCHEMA_KEY_PREFIX);
        UnsignedIntEncoder.write(writer, version);
        return writer.getBytes();
    }

    /**
     * Get the common prefix of all object version index entries.
     *
     * @return object version index key prefix
     */
    public static byte[] getObjectVersionIndexKeyPrefix() {
        return VERSION_INDEX_PREFIX.clone();
    }

    /**
     * Get the common prefix of all user-defined meta-data keys.
     *
     * @return user meta-data key prefix
     */
    public static byte[] getUserMetaDataKeyPrefix() {
        return USER_META_DATA_KEY_PREFIX.clone();
    }

    /**
     * Get a {@link CoreIndex} view of the object version index in a key/value database.
     *
     * @param kv key/value data
     * @return object version index
     * @throws IllegalArgumentException if {@code kv} is null
     */
    public static CoreIndex<Integer, ObjId> getVersionIndex(KVStore kv) {
        Preconditions.checkArgument(kv != null, "null kv");
        return new CoreIndex<>(kv,
          new IndexView<>(VERSION_INDEX_PREFIX, false, FieldTypeRegistry.UNSIGNED_INT, FieldTypeRegistry.OBJ_ID));
    }

    /**
     * Build the key for an object version index entry.
     *
     * @param id object ID
     * @param version object version
     * @return version index entry key
     * @throws IllegalArgumentException if {@code id} is null
     * @throws IllegalArgumentException if {@code version} is non-positive
     */
    public static byte[] buildVersionIndexKey(ObjId id, int version) {
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(version > 0, "non-positive version");
        final ByteWriter writer = new ByteWriter(VERSION_INDEX_PREFIX.length + 5 + ObjId.NUM_BYTES);
        writer.write(VERSION_INDEX_PREFIX);
        UnsignedIntEncoder.write(writer, version);
        id.writeTo(writer);
        return writer.getBytes();
    }

    /**
     * Decode schema XML from a schema version meta-data entry.
     *
     * @param value encoded schema from key/value pair
     * @param formatVersion database format version
     * @return decoded schema
     * @throws InvalidSchemaException if schema is invalid
     * @throws IllegalArgumentException if {@code formatVersion} is invalid
     * @throws IllegalArgumentException if {@code value} is null
     */
    public static SchemaModel decodeSchema(byte[] value, int formatVersion) {

        // Sanity check
        Preconditions.checkArgument(value != null, "null value");

        // Compressed XML?
        final boolean compressed;
        switch (formatVersion) {
        case FORMAT_VERSION_1:
        case FORMAT_VERSION_2:
            compressed = formatVersion >= FORMAT_VERSION_2;
            break;
        default:
            throw new IllegalArgumentException("unrecognized format version " + formatVersion);
        }

        // Decompress if needed
        if (compressed) {
            try {
                final Inflater inflater = new Inflater(true);
                inflater.setDictionary(SCHEMA_XML_COMPRESSION_DICTIONARY);
                inflater.setInput(Bytes.concat(value, new byte[1]));
                final ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
                final byte[] temp = new byte[1000];
                int r;
                while ((r = inflater.inflate(temp)) != 0)
                    decompressed.write(temp, 0, r);
                if (!inflater.finished())
                    throw new RuntimeException("internal error: inflater did not finish");
                inflater.end();
                value = decompressed.toByteArray();
            } catch (DataFormatException e) {
                throw new InvalidSchemaException("error in compressed schema XML data", e);
            }
        }

        // Decode XML
        try {
            return SchemaModel.fromXML(new ByteArrayInputStream(value));
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }
    }

    /**
     * Encode schema XML for a schema version meta-data entry.
     *
     * @param schemaModel schema
     * @param formatVersion database format version
     * @return encoded schema for key/value pair
     * @throws IllegalArgumentException if {@code formatVersion} is invalid
     * @throws IllegalArgumentException if {@code schemaModel} is null
     */
    public static byte[] encodeSchema(SchemaModel schemaModel, int formatVersion) {

        // Sanity check
        Preconditions.checkArgument(schemaModel != null, "null schemaModel");

        // Compress XML?
        final boolean compress;
        switch (formatVersion) {
        case FORMAT_VERSION_1:
        case FORMAT_VERSION_2:
            compress = formatVersion >= FORMAT_VERSION_2;
            break;
        default:
            throw new IllegalArgumentException("unrecognized format version " + formatVersion);
        }

        // Encode as XML
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            schemaModel.toXML(buf, false);
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }
        byte[] value = buf.toByteArray();

        // Compress if needed
        if (compress) {
            final Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION, true);
            deflater.setDictionary(SCHEMA_XML_COMPRESSION_DICTIONARY);
            deflater.setInput(value);
            deflater.finish();
            final ByteArrayOutputStream compressed = new ByteArrayOutputStream();
            final byte[] temp = new byte[1000];
            int r;
            while ((r = deflater.deflate(temp)) != 0)
                compressed.write(temp, 0, r);
            if (!deflater.finished())
                throw new RuntimeException("internal error: deflater did not finish");
            deflater.end();
            value = compressed.toByteArray();
        }

        // Done
        return value;
    }

    /**
     * Get the key in the meta-data area corresponding to the schema with the given version number.
     *
     * @param version schema version number
     * @return key for schema version
     * @throws IllegalArgumentException if {@code version} is non-positive
     */
    public static byte[] getSchemaKey(int version) {
        Preconditions.checkArgument(version > 0, "non-positive version");
        final ByteWriter writer = new ByteWriter(SCHEMA_KEY_PREFIX.length + 5);
        writer.write(SCHEMA_KEY_PREFIX);
        UnsignedIntEncoder.write(writer, version);
        return writer.getBytes();
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
        final KeyRange versionIndexRange = KeyRange.forPrefix(Layout.getObjectVersionIndexKeyPrefix());
        assert metaDataRange.contains(versionIndexRange);

        // Delete everything except meta-data
        kv.removeRange(null, metaDataRange.getMin());
        kv.removeRange(metaDataRange.getMax(), null);

        // Delete the object version index
        kv.removeRange(metaDataRange.getMin(), versionIndexRange.getMin());
        kv.removeRange(versionIndexRange.getMax(), metaDataRange.getMax());
    }

    /**
     * Copy non-object meta-data from one {@link KVStore} to another.
     *
     * <p>
     * This copies all meta-data except the object version index. Any existing key/value pairs
     * in the destination meta-data range are not removed prior to the copy.
     *
     * @param src source key/value database
     * @param dst destination key/value database
     */
    public static void copyMetaData(KVStore src, KVStore dst) {

        // Get ranges
        final KeyRange metaDataRange = KeyRange.forPrefix(Layout.getMetaDataKeyPrefix());
        final KeyRange versionIndexRange = KeyRange.forPrefix(Layout.getObjectVersionIndexKeyPrefix());
        assert metaDataRange.contains(versionIndexRange);

        // Copy meta-data
        Layout.copyRange(src, dst, metaDataRange.getMin(), versionIndexRange.getMin());
        Layout.copyRange(src, dst, versionIndexRange.getMax(), metaDataRange.getMax());
    }

    private static void copyRange(KVStore src, KVStore dst, byte[] minKey, byte[] maxKey) {
        try (final CloseableIterator<KVPair> i = src.getRange(minKey, maxKey)) {
            while (i.hasNext()) {
                final KVPair pair = i.next();
                dst.put(pair.getKey(), pair.getValue());
            }
        }
    }
}

