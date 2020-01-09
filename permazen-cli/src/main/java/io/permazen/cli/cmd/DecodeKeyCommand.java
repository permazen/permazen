
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import com.google.common.base.Preconditions;

import io.permazen.Session;
import io.permazen.cli.CliSession;
import io.permazen.core.ComplexField;
import io.permazen.core.CompositeIndex;
import io.permazen.core.Field;
import io.permazen.core.FieldSwitchAdapter;
import io.permazen.core.FieldType;
import io.permazen.core.Layout;
import io.permazen.core.ListField;
import io.permazen.core.MapField;
import io.permazen.core.ObjId;
import io.permazen.core.ObjType;
import io.permazen.core.Schema;
import io.permazen.core.SchemaItem;
import io.permazen.core.Schemas;
import io.permazen.core.SetField;
import io.permazen.core.SimpleField;
import io.permazen.core.Transaction;
import io.permazen.core.UnknownFieldException;
import io.permazen.core.type.ReferenceFieldType;
import io.permazen.parse.Parser;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ParseContext;
import io.permazen.util.UnsignedIntEncoder;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
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
        return "This command takes a byte[] array or hexadecimal string containing a key/value key, and attempts to decode"
          + " what the key represents given the currently configured schema. If multiple byte[] arrays are given, they are"
          + " concatenated into a single key.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "bytes".equals(typeName) ? new AbstractKVCommand.BytesParser() : super.getParser(typeName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public DecodeKeyAction getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return new DecodeKeyAction((List<byte[]>)params.get("bytes"));
    }

    /**
     * Decode a key/value in the context of a {@link Transaction}.
     *
     * @param tx transaction
     * @param key key to decode
     * @return description of decoded key
     * @throws IllegalArgumentException if either parameter is null
     */
    public static String decode(Transaction tx, byte[] key) {

        // Sanity check
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(key != null, "null key");

        // Empty?
        if (key.length == 0)
            return "Empty byte array";

        // Get info
        final Schemas schemas = tx.getSchemas();
        final HashMap<Integer, SchemaItem> storageIdMap = new HashMap<>();
        final HashMap<Integer, ComplexField<?>> parentMap = new HashMap<>();
        for (Schema schema : schemas.getVersions().values()) {
            for (ObjType objType : schema.getObjTypes().values()) {
                storageIdMap.put(objType.getStorageId(), objType);
                for (CompositeIndex index : objType.getCompositeIndexes().values())
                    storageIdMap.put(index.getStorageId(), index);
                for (Field<?> field0 : objType.getFields().values()) {
                    field0.visit(new FieldSwitchAdapter<Void>() {

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
                        protected <T> Void caseField(Field<T> field) {
                            return null;
                        }
                    });
                }
            }
        }

        // Decode
        final ByteReader reader = new ByteReader(key);
        final Decodes decodes = new Decodes(reader, storageIdMap);
        try {
            while (true) {

                // Handle meta-data
                if (reader.readByte() == 0x00) {
                    decodes.add("Meta-data key prefix");
                    switch (reader.readByte()) {
                    case 0x00:
                        decodes.add("Format version prefix");
                        if (Arrays.equals(reader.getBytes(), Layout.getFormatVersionKey()))
                            decodes.addRemainder("Format version key");
                        break;
                    case 0x01:
                        decodes.add("Recorded schemas prefix");
                        decodes.add("Schema version #" + UnsignedIntEncoder.read(reader));
                        break;
                    case 0x80:
                        decodes.add("Object version index");
                        decodes.add("Object version #" + UnsignedIntEncoder.read(reader));
                        decodes.add(new ObjId(reader));
                        break;
                    case 0xff:
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
                        parent.visit(new FieldSwitchAdapter<Void>() {

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
                            protected <T> Void caseField(Field<T> field) {
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
                field.visit(new FieldSwitchAdapter<Void>() {

                    @Override
                    public <E> Void caseListField(ListField<E> field) {
                        super.caseListField(field);
                        decodes.add("List index " + UnsignedIntEncoder.read(reader));
                        return null;
                    }

                    @Override
                    public <E> Void caseSetField(SetField<E> field) {
                        super.caseSetField(field);
                        decodes.add("Set element", field.getElementField(), reader);
                        return null;
                    }

                    @Override
                    public <K, V> Void caseMapField(MapField<K, V> field) {
                        super.caseMapField(field);
                        decodes.add("Map key ", field.getKeyField(), reader);
                        return null;
                    }

                    @Override
                    protected <T> Void caseField(Field<T> field) {
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

    private static class DecodeKeyAction implements CliSession.Action, Session.RetryableAction {

        private final List<byte[]> bytesList;

        DecodeKeyAction(List<byte[]> bytesList) {
            this.bytesList = bytesList;
        }

        @Override
        public void run(CliSession session) throws Exception {

            // Get info
            final PrintWriter writer = session.getWriter();
            final Transaction tx = session.getTransaction();

            // Concatenate byte[] arrays
            final ByteArrayOutputStream buf = new ByteArrayOutputStream();
            for (byte[] bytes : this.bytesList)
                buf.write(bytes);

            // Decode and print
            writer.print(DecodeKeyCommand.decode(tx, buf.toByteArray()));
        }
    }

    private static class Decodes {

        private final Map<Integer, SchemaItem> storageIdMap;
        private final ByteReader reader;
        private final ArrayList<Decode> decodeList = new ArrayList<>();

        private int lastOffset;

        Decodes(ByteReader reader, Map<Integer, SchemaItem> storageIdMap) {
            Preconditions.checkArgument(reader != null, "null reader");
            Preconditions.checkArgument(storageIdMap != null, "null storageIdMap");
            this.reader = reader;
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
        <T> void add(String label, SimpleField<T> field, ByteReader reader) {
            final FieldType<T> fieldType = field.getFieldType();
            final T value = fieldType.read(reader);
            if (fieldType instanceof ReferenceFieldType)
                this.add(label, (ObjId)value);
            else
                this.add(label + " " + fieldType.toParseableString(value));
        }

        void addRemainder(String description) {
            this.reader.readBytes(this.reader.remain());
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
            int off = 0;
            for (Decode decode : this.decodeList) {
                final int len = decode.getLength();
                buf.append(String.format(format, this.truncate(reader.getBytes(off, len), byteLimit), decode.getDescription()));
                off += len;
            }
            return buf.toString();
        }

        private String truncate(byte[] array, int maxBytes) {
            Preconditions.checkArgument(array != null, "null array");
            Preconditions.checkArgument(maxBytes > 3, "maxBytes < 4");
            if (array.length <= maxBytes)
                return ByteUtil.toString(array);
            return String.format("%s..%s",
              ByteUtil.toString(Arrays.copyOfRange(array, 0, maxBytes - 4)),
              ByteUtil.toString(Arrays.copyOfRange(array, array.length - 3, array.length)));
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
