
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.cli.parse.Parser;
import io.permazen.core.ComplexField;
import io.permazen.core.CompositeIndex;
import io.permazen.core.Field;
import io.permazen.core.FieldSwitch;
import io.permazen.core.Layout;
import io.permazen.core.ListField;
import io.permazen.core.MapField;
import io.permazen.core.ObjId;
import io.permazen.core.ObjType;
import io.permazen.core.ReferenceEncoding;
import io.permazen.core.Schema;
import io.permazen.core.SchemaBundle;
import io.permazen.core.SchemaItem;
import io.permazen.core.SetField;
import io.permazen.core.SimpleField;
import io.permazen.core.Transaction;
import io.permazen.core.UnknownFieldException;
import io.permazen.encoding.Encoding;
import io.permazen.util.ByteData;
import io.permazen.util.UnsignedIntEncoder;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DecodeKeyCommand extends AbstractKVCommand {

    public DecodeKeyCommand() {
        super("decode-key bytes:bytes+");
    }

    @Override
    public String getHelpSummary() {
        return "Decodes a key in the key/value store";
    }

    @Override
    public String getHelpDetail() {
        return "This command takes a byte[] array or hexadecimal string containing a key and attempts to decode"
          + " what the key represents in the key/value store given the currently configured schema. If multiple"
          + " byte[] arrays are given, they are concatenated into a single key.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "bytes".equals(typeName) ? new AbstractKVCommand.BytesParser() : super.getParser(typeName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public DecodeKeyAction getAction(Session session, Map<String, Object> params) {
        return new DecodeKeyAction((List<ByteData>)params.get("bytes"));
    }

    /**
     * Decode a key/value in the context of a {@link Transaction}.
     *
     * @param tx transaction
     * @param key key to decode
     * @return description of decoded key
     * @throws IllegalArgumentException if either parameter is null
     */
    public static String decode(Transaction tx, ByteData key) {

        // Sanity check
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(key != null, "null key");

        // Empty?
        if (key.size() == 0)
            return "Empty byte array";

        // Get info
        final SchemaBundle schemaBundle = tx.getSchemaBundle();
        final HashMap<Integer, SchemaItem> storageIdMap = new HashMap<>();
        final HashMap<Integer, ComplexField<?>> parentMap = new HashMap<>();
        for (Schema schema : schemaBundle.getSchemasBySchemaId().values()) {
            for (ObjType objType : schema.getObjTypes().values()) {
                storageIdMap.put(objType.getStorageId(), objType);
                for (CompositeIndex index : objType.getCompositeIndexes().values())
                    storageIdMap.put(index.getStorageId(), index);
                for (Field<?> field0 : objType.getFields().values()) {
                    field0.visit(new FieldSwitch<Void>() {

                        @Override
                        public <T> Void caseSimpleField(SimpleField<T> field) {
                            if (field.isIndexed())
                                storageIdMap.put(field.getStorageId(), field);
                            return null;
                        }

                        @Override
                        public <T> Void caseComplexField(ComplexField<T> field) {
                            for (SimpleField<?> subField : field.getSubFields()) {
                                parentMap.put(subField.getStorageId(), field);
                                this.caseSimpleField(subField);
                            }
                            return null;
                        }

                        @Override
                        public <T> Void caseField(Field<T> field) {
                            return null;
                        }
                    });
                }
            }
        }

        // Decode
        final ByteData.Reader reader = key.newReader();
        final Decodes decodes = new Decodes(reader, storageIdMap);
        try {
            while (true) {

                // Handle meta-data
                if (reader.readByte() == Layout.METADATA_PREFIX_BYTE) {
                    decodes.add("Meta-data key prefix");
                    switch (reader.readByte()) {
                    case Layout.METADATA_FORMAT_VERSION_BYTE:
                        decodes.add("Format version prefix");
                        if (reader.getByteData().equals(Layout.getFormatVersionKey()))
                            decodes.addRemainder("Format version key");
                        break;
                    case Layout.METADATA_SCHEMA_TABLE_BYTE:
                        decodes.add("Schema table prefix");
                        decodes.add("Schema index #" + UnsignedIntEncoder.read(reader));
                        break;
                    case Layout.METADATA_STORAGE_ID_TABLE_BYTE:
                        decodes.add("Storage ID table prefix");
                        decodes.add("Storage ID #" + UnsignedIntEncoder.read(reader));
                        break;
                    case Layout.METADATA_SCHEMA_INDEX_BYTE:
                        decodes.add("Object schema index");
                        decodes.add("Schema index #" + UnsignedIntEncoder.read(reader));
                        decodes.add(new ObjId(reader));
                        break;
                    case Layout.METADATA_USER_META_DATA_BYTE:
                        decodes.add("User meta-data range");
                        decodes.addRemainder("User meta-data key");
                        break;
                    default:
                        decodes.add("Unknown meta-data prefix");
                        break;
                    }
                    break;
                }
                reader.unread();

                // Get storage ID
                final int storageId = UnsignedIntEncoder.read(reader);
                final SchemaItem schemaItem = storageIdMap.get(storageId);
                if (schemaItem == null) {
                    decodes.add("Unknown storage ID " + storageId);
                    break;
                }

                // Handle composite index
                if (schemaItem instanceof CompositeIndex) {
                    final CompositeIndex index = (CompositeIndex)schemaItem;

                    // Describe storage ID
                    final StringBuilder buf = new StringBuilder();
                    for (SimpleField<?> field : index.getFields()) {
                        if (buf.length() == 0) {
                            buf.append("[#").append(storageId).append("] Composite index \"")
                              .append(index.getName()).append("\" on ");
                        } else
                            buf.append(", ");
                        buf.append(field);
                    }
                    decodes.add(buf);

                    // Describe indexed values
                    for (SimpleField<?> field : index.getFields())
                        decodes.add("Value", field, reader);

                    // Describe object ID
                    decodes.add(new ObjId(reader));
                    break;
                }

                // Handle simple index
                if (schemaItem instanceof SimpleField) {
                    final SimpleField<?> field = (SimpleField<?>)schemaItem;

                    // Describe storage ID
                    decodes.add("[#" + storageId + "] Simple index on " + field);

                    // Describe value
                    decodes.add("Value", field, reader);

                    // Describe object ID
                    decodes.add(new ObjId(reader));

                    // Describe list index or key
                    final ComplexField<?> parent = parentMap.get(field.getStorageId());
                    if (parent != null) {
                        parent.visit(new FieldSwitch<Void>() {

                            @Override
                            public <E> Void caseListField(ListField<E> field) {
                                decodes.add("List index " + UnsignedIntEncoder.read(reader));
                                return null;
                            }

                            @Override
                            public <K, V> Void caseMapField(MapField<K, V> field) {
                                decodes.add("Map key", field.getKeyField(), reader);
                                return null;
                            }

                            @Override
                            public <T> Void caseField(Field<T> field) {
                                return null;
                            }
                        });
                    }
                    break;
                }

                // Handle object
                final ObjType objType = (ObjType)schemaItem;
                decodes.add("[#" + storageId + "] " + objType);
                reader.readBytes(ObjId.NUM_BYTES - reader.getOffset());
                decodes.add("Random/unique part of ObjId");
                if (reader.remain() == 0)
                    break;

                // Handle object field value
                final int fieldStorageId = UnsignedIntEncoder.read(reader);
                final Field<?> field;
                try {
                    field = objType.getField(fieldStorageId);
                } catch (UnknownFieldException e) {
                    decodes.add("Invalid storage ID " + fieldStorageId);
                    break;
                }
                field.visit(new FieldSwitch<Void>() {

                    @Override
                    public <E> Void caseListField(ListField<E> field) {
                        FieldSwitch.super.caseListField(field);
                        decodes.add("List index " + UnsignedIntEncoder.read(reader));
                        return null;
                    }

                    @Override
                    public <E> Void caseSetField(SetField<E> field) {
                        FieldSwitch.super.caseSetField(field);
                        decodes.add("Set element", field.getElementField(), reader);
                        return null;
                    }

                    @Override
                    public <K, V> Void caseMapField(MapField<K, V> field) {
                        FieldSwitch.super.caseMapField(field);
                        decodes.add("Map key ", field.getKeyField(), reader);
                        return null;
                    }

                    @Override
                    public <T> Void caseField(Field<T> field) {
                        decodes.add("[#" + fieldStorageId + "] " + field);
                        return null;
                    }
                });
                break;
            }
        } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
            // we encountered a truncated key or some kind of uninterpretable garbage
        }

        // Whatever remains (if anything) is garbage
        decodes.addRemainder("Trailing garbage");

        // Done
        return decodes.toString();
    }

    private static class DecodeKeyAction implements Session.RetryableTransactionalAction {

        private final List<ByteData> bytesList;

        DecodeKeyAction(List<ByteData> bytesList) {
            this.bytesList = bytesList;
        }

        @Override
        public SessionMode getTransactionMode(Session session) {
            return SessionMode.CORE_API;
        }

        @Override
        public void run(Session session) throws Exception {

            // Get info
            final PrintStream writer = session.getOutput();
            final Transaction tx = session.getTransaction();

            // Concatenate byte[] arrays
            final ByteData bytes = bytesList.stream().reduce(ByteData.empty(), ByteData::concat);

            // Decode and print
            writer.print(DecodeKeyCommand.decode(tx, bytes));
        }
    }

    private static class Decodes {

        private final Map<Integer, SchemaItem> storageIdMap;
        private final ByteData.Reader reader;
        private final ArrayList<Decode> decodeList = new ArrayList<>();

        private final int startingOffset;
        private int lastOffset;

        Decodes(ByteData.Reader reader, Map<Integer, SchemaItem> storageIdMap) {
            Preconditions.checkArgument(reader != null, "null reader");
            Preconditions.checkArgument(storageIdMap != null, "null storageIdMap");
            this.reader = reader;
            this.startingOffset = reader.getOffset();
            this.lastOffset = this.startingOffset;
            this.storageIdMap = storageIdMap;
        }

        // Add arbitrary description
        void add(Object description) {
            Preconditions.checkArgument(description != null, "null description");
            final int len = this.reader.getOffset() - this.lastOffset;
            Preconditions.checkState(len > 0, "no bytes have been read");
            this.decodeList.add(new Decode(len, description.toString()));
            this.lastOffset = this.reader.getOffset();
        }

        // Add object ID
        void add(ObjId id) {
            this.add(null, id);
        }

        private void add(String label, ObjId id) {
            Preconditions.checkArgument(id != null, "null id");
            final int storageId = id.getStorageId();
            final SchemaItem schemaItem = this.storageIdMap.get(storageId);
            if (schemaItem instanceof ObjType) {
                final ObjType objType = (ObjType)schemaItem;
                this.add((label != null ? label + " " : "") + "Object ID " + id + " of type \"" + objType.getName() + "\"");
            } else
                this.add((label != null ? label + " " : "") + "Object ID " + id + " (invalid type #" + storageId + ")");
        }

        // Add a simple field value
        <T> void add(String label, SimpleField<T> field, ByteData.Reader reader) {
            final Encoding<T> encoding = field.getEncoding();
            final T value = encoding.read(reader);
            if (encoding instanceof ReferenceEncoding)
                this.add(label, (ObjId)value);
            else if (value == null)
                this.add(label + " null");
            else
                this.add(label + " " + encoding.toString(value));
        }

        void addRemainder(String description) {
            this.reader.readRemaining();
            if (this.reader.getOffset() > this.lastOffset)
                this.add(description);
        }

        @Override
        public String toString() {
            int maxLength = 0;
            for (Decode decode : this.decodeList)
                maxLength = Math.max(maxLength, decode.getLength());
            final int byteLimit = 32;
            assert byteLimit > 3;
            final int maxBytes = Math.min(maxLength, byteLimit);
            final StringBuilder buf = new StringBuilder();
            final String format = String.format("%%%ds : %%s%%n", maxBytes * 2);
            int off = this.startingOffset;
            for (Decode decode : this.decodeList) {
                final int len = decode.getLength();
                final ByteData decodeData = reader.getByteData().substring(off, len);
                buf.append(String.format(format, this.truncate(decodeData, byteLimit), decode.getDescription()));
                off += len;
            }
            return buf.toString();
        }

        private String truncate(ByteData data, int maxBytes) {
            Preconditions.checkArgument(data != null, "null array");
            Preconditions.checkArgument(maxBytes > 3, "maxBytes < 4");
            return data.toHex(maxBytes);
        }
    }

    private static class Decode {

        private final int len;
        private final String description;

        Decode(int len, String description) {
            Preconditions.checkArgument(len > 0, "bogus length");
            Preconditions.checkArgument(description != null, "null description");
            this.len = len;
            this.description = description;
        }

        int getLength() {
            return this.len;
        }

        String getDescription() {
            return this.description;
        }
    }
}
