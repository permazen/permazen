
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import com.google.common.collect.PeekingIterator;

import io.permazen.core.ComplexField;
import io.permazen.core.CompositeIndex;
import io.permazen.core.CounterField;
import io.permazen.core.Encodings;
import io.permazen.core.Field;
import io.permazen.core.FieldSwitch;
import io.permazen.core.Layout;
import io.permazen.core.ListField;
import io.permazen.core.MapField;
import io.permazen.core.ObjId;
import io.permazen.core.ObjType;
import io.permazen.core.ReferenceEncoding;
import io.permazen.core.ReferenceField;
import io.permazen.core.SetField;
import io.permazen.core.SimpleField;
import io.permazen.core.UnknownFieldException;
import io.permazen.encoding.Encoding;
import io.permazen.kv.KVPair;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;

import java.util.HashMap;
import java.util.HashSet;

class ObjectType extends Storage<ObjType> {

    // Derived info
    private final HashSet<SimpleField<?>> indexedSimpleFields = new HashSet<>();                // does not include sub-fields

    ObjectType(JsckInfo info, ObjType objType) {
        super(info, objType);

        // Inventory indexed simple fields
        objType.getFields().forEach((name, field) -> {
            if (field instanceof SimpleField) {
                final SimpleField<?> simpleField = (SimpleField<?>)field;
                if (simpleField.isIndexed())
                    this.indexedSimpleFields.add(simpleField);
            }
        });
    }

    /**
     * Validate the encoding of an object.
     *
     * @param info runtime info
     * @param id object ID
     * @param schemaIndex object schema index
     * @param i iteration of all key/value pairs having {@code id} as a strict prefix
     * @throws IllegalArgumentException if entry is invalid
     */
    public void validateObjectData(JsckInfo info, ObjId id, int schemaIndex, PeekingIterator<KVPair> i) {

        // Get object info
        final byte[] objectPrefix = id.getBytes();

        // Keep track of which simple fields we see with non-default values
        final HashSet<SimpleField<?>> indexedSimpleFieldsWithDefaultValues = new HashSet<>(this.indexedSimpleFields);

        // Keep track of simple field values (after possible fixups)
        final HashMap<String, byte[]> simpleFieldValues = new HashMap<>();

        // Scan field data; note we will not see simple fields with default values in this loop
        while (i.hasNext()) {
            final KVPair pair = i.peek();
            final byte[] key = pair.getKey();

            // Have we reached the end of the object?
            if (!ByteUtil.isPrefixOf(objectPrefix, key))
                break;
            assert key.length > ObjId.NUM_BYTES;

            // Decode field storage ID
            final ByteReader keyReader = new ByteReader(key, ObjId.NUM_BYTES);
            final int storageId;
            try {
                storageId = Encodings.UNSIGNED_INT.read(keyReader);
            } catch (IllegalArgumentException e) {
                info.handle(new InvalidKey(pair).setDetail(id, "invalid field storage ID"));
                continue;
            }
            if (storageId <= 0) {
                info.handle(new InvalidKey(pair).setDetail(id, "invalid field storage ID " + storageId));
                continue;
            }

            // Find the field
            final Field<?> field;
            try {
                field = this.schemaItem.getField(storageId);
            } catch (UnknownFieldException e) {
                info.handle(new InvalidKey(pair).setDetail(id, String.format(
                  "invalid field storage ID %d: no such field exists in %s", storageId, this.schemaItem)));
                continue;
            }

            // Build field prefix
            final byte[] fieldPrefix = keyReader.getBytes(0, keyReader.getOffset());

            // Scan field
            if (info.isDetailEnabled())
                info.detail("checking object %s %s", id, field);
            field.visit(new FieldSwitch<Void>() {
                @Override
                public <T> Void caseSimpleField(SimpleField<T> field) {
                    final byte[] value = ObjectType.this.checkSimpleField(info, id, field, fieldPrefix, i);
                    simpleFieldValues.put(field.getName(), value);
                    if (value != null)
                        indexedSimpleFieldsWithDefaultValues.remove(field);
                    return null;
                }
                @Override
                public <E> Void caseSetField(SetField<E> field) {
                    ObjectType.this.checkSetField(info, id, field, fieldPrefix, i);
                    return null;
                }
                @Override
                public <E> Void caseListField(ListField<E> field) {
                    ObjectType.this.checkListField(info, id, field, fieldPrefix, i);
                    return null;
                }
                @Override
                public <K, V> Void caseMapField(MapField<K, V> field) {
                    ObjectType.this.checkMapField(info, id, field, fieldPrefix, i);
                    return null;
                }
                @Override
                public Void caseCounterField(CounterField field) {
                    ObjectType.this.checkCounterField(info, id, field, fieldPrefix, i);
                    return null;
                }
            });
        }
        assert !i.hasNext() || !ByteUtil.isPrefixOf(objectPrefix, i.peek().getKey());

        // Verify index entries for indexed simple fields that had default values (which we would not have encountered)
        for (SimpleField<?> field : indexedSimpleFieldsWithDefaultValues) {
            final Encoding<?> encoding = field.getEncoding();
            final byte[] defaultValue = encoding.getDefaultValueBytes();
            this.verifySimpleIndexEntry(info, id, field, defaultValue);
        }

        // Verify composite index entries
        for (CompositeIndex index : this.schemaItem.getCompositeIndexes().values())
            this.verifyCompositeIndexEntry(info, id, index, simpleFieldValues);

        // Verify object schema index entry
        this.verifySchemaIndexEntry(info, id, schemaIndex);
    }

    // Returns field's byte[] value if field has non-default value, otherwise null
    private <T> byte[] checkSimpleField(JsckInfo info, ObjId id, SimpleField<T> field, byte[] prefix, PeekingIterator<KVPair> i) {

        // Get encoding
        final Encoding<?> encoding = field.getEncoding();
        assert encoding != null;

        // Get field key/value pair
        final KVPair pair = i.next();
        assert pair != null;
        assert ByteUtil.isPrefixOf(prefix, pair.getKey());

        // Check for trailing garbage in key
        if (pair.getKey().length > prefix.length) {
            info.handle(new InvalidKey(pair).setDetail(id, field,
              String.format("trailing garbage %s", Jsck.ds(new ByteReader(pair.getKey(), prefix.length)))));
            return null;
        }

        // Decode value
        byte[] value = pair.getValue();
        final ByteReader reader = new ByteReader(pair.getValue());
        if (!this.validateSimpleFieldValue(info, id, field, pair, reader))
            value = null;

        // We should not see default values in simple fields that are not sub-fields of complex fields
        if (value != null && ByteUtil.compare(value, encoding.getDefaultValueBytes()) == 0) {
            info.handle(new InvalidValue(pair).setDetail("default value; should not be present"));
            value = null;
        }

        // Verify index entry
        if (field.isIndexed())
            this.verifySimpleIndexEntry(info, id, field, value != null ? value : encoding.getDefaultValueBytes());

        // Done
        return value;
    }

    private <E> void checkSetField(JsckInfo info, ObjId id, SetField<E> field, byte[] prefix, PeekingIterator<KVPair> i) {

        // Get element field
        final SimpleField<E> elementField = field.getElementField();

        // Iterate over set elements
        while (i.hasNext() && ByteUtil.isPrefixOf(prefix, i.peek().getKey())) {
            final KVPair pair = i.next();

            // Verify encoded element
            final ByteReader reader = new ByteReader(pair.getKey(), prefix.length);
            if (!this.validateSimpleFieldValue(info, id, elementField, pair, reader))
                continue;

            // Value should be empty
            if (pair.getValue().length != 0)
                info.handle(new InvalidValue(pair, ByteUtil.EMPTY).setDetail(id, elementField, "should be empty"));

            // Verify index entry
            if (elementField.isIndexed())
                this.verifySimpleIndexEntry(info, id, elementField, field, reader.getBytes(prefix.length), ByteUtil.EMPTY);
        }
    }

    private <K, V> void checkMapField(JsckInfo info, ObjId id, MapField<K, V> field, byte[] prefix, PeekingIterator<KVPair> i) {

        // Get key and value fields
        final SimpleField<K> keyField = field.getKeyField();
        final SimpleField<V> valField = field.getValueField();

        // Iterate over set elements
        while (i.hasNext() && ByteUtil.isPrefixOf(prefix, i.peek().getKey())) {
            final KVPair pair = i.next();

            // Verify encoded key
            final ByteReader keyReader = new ByteReader(pair.getKey(), prefix.length);
            if (!this.validateSimpleFieldValue(info, id, keyField, pair, keyReader))
                continue;

            // Verify encoded value
            final ByteReader valReader = new ByteReader(pair.getValue());
            if (!this.validateSimpleFieldValue(info, id, valField, pair, valReader))
                continue;

            // Verify index entries
            if (keyField.isIndexed())
                this.verifySimpleIndexEntry(info, id, keyField, field, keyReader.getBytes(prefix.length), ByteUtil.EMPTY);
            if (valField.isIndexed())
                this.verifySimpleIndexEntry(info, id, valField, field, pair.getValue(), keyReader.getBytes(prefix.length));
        }
    }

    private <E> void checkListField(JsckInfo info, ObjId id, ListField<E> field, byte[] prefix, PeekingIterator<KVPair> i) {

        // Get element field and type
        final SimpleField<E> elementField = field.getElementField();

        // Iterate over list elements
        int expectedIndex = 0;
        while (i.hasNext() && ByteUtil.isPrefixOf(prefix, i.peek().getKey())) {
            final KVPair pair = i.next();

            // Decode list index
            final ByteReader keyReader = new ByteReader(pair.getKey(), prefix.length);
            final int actualIndex;
            try {
                try {
                    actualIndex = Encodings.UNSIGNED_INT.read(keyReader);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("invalid list index: " + e.getMessage(), e);
                }
                if (keyReader.remain() > 0) {
                    throw new IllegalArgumentException(String.format(
                      "trailing garbage %s after encoded index %d",
                      Jsck.ds(keyReader, keyReader.getOffset()), actualIndex));
                }
            } catch (IllegalArgumentException e) {
                info.handle(new InvalidValue(pair).setDetail(id, elementField, e.getMessage()));
                continue;
            }

            // Verify encoded element
            final ByteReader valReader = new ByteReader(pair.getValue());
            if (!this.validateSimpleFieldValue(info, id, elementField, pair, valReader))
                continue;

            // Check list index, and renumber if necessary
            byte[] encodedIndex = keyReader.getBytes(prefix.length);
            if (actualIndex != expectedIndex) {
                info.handle(new InvalidValue(pair).setDetail(id,
                  elementField, String.format("wrong index %d != %d", actualIndex, expectedIndex)));
                final ByteWriter keyWriter = new ByteWriter();
                keyWriter.write(prefix);
                Encodings.UNSIGNED_INT.write(keyWriter, expectedIndex);
                encodedIndex = keyWriter.getBytes(prefix.length);
                info.handle(new MissingKey("incorrect list index", keyWriter.getBytes(), pair.getValue())
                  .setDetail(id, elementField, String.format("renumbered list index %d -> %d", actualIndex, expectedIndex)));
            }

            // Entry is good - we can advance the list index
            expectedIndex++;

            // Verify index entry
            if (elementField.isIndexed())
                this.verifySimpleIndexEntry(info, id, elementField, field, pair.getValue(), encodedIndex);
        }
    }

    private void checkCounterField(JsckInfo info, ObjId id, CounterField field, byte[] prefix, PeekingIterator<KVPair> i) {

        // Get field key/value pair
        final KVPair pair = i.next();
        assert pair != null;
        assert ByteUtil.isPrefixOf(prefix, pair.getKey());

        // Check for trailing garbage in key
        if (pair.getKey().length > prefix.length) {
            info.handle(new InvalidKey(pair).setDetail(id, field,
              String.format("trailing garbage %s", Jsck.ds(new ByteReader(pair.getKey(), prefix.length)))));
            return;
        }

        // Decode value
        try {
            info.getKVStore().decodeCounter(pair.getValue());
        } catch (IllegalArgumentException e) {
            info.handle(new InvalidValue(pair).setDetail(id, field, " (resetting to zero): " + e.getMessage()));
        }
    }

    private <T> boolean validateSimpleFieldValue(JsckInfo info, ObjId id, SimpleField<T> field, KVPair pair, ByteReader reader) {

        // Verify field encoding
        final Encoding<T> encoding = field.getEncoding();
        try {

            // Decode value
            final T value = encoding.read(reader);
            if (reader.remain() > 0)
                throw new IllegalArgumentException(String.format("trailing garbage %s", Jsck.ds(reader, reader.getOffset())));

            // For reference fields, check for illegal dangling references
            if (value != null && field instanceof ReferenceField) {
                final ReferenceField referenceField = (ReferenceField)field;
                if (!referenceField.isAllowDeleted()) {
                    assert encoding instanceof ReferenceEncoding;
                    final ObjId target = (ObjId)value;
                    if (info.getKVStore().get(target.getBytes()) == null)
                        throw new IllegalArgumentException("invalid reference to deleted object " + target);
                }
            }
        } catch (IllegalArgumentException e) {
            info.handle(new InvalidValue(pair).setDetail(id, field, e.getMessage()));
            return false;
        }
        return true;
    }

    private <T> void verifySimpleIndexEntry(JsckInfo info, ObjId id, SimpleField<T> field, byte[] value) {
        this.verifySimpleIndexEntry(info, id, field.getStorageId(), "" + field.getIndex(), value, ByteUtil.EMPTY);
    }

    private void verifySimpleIndexEntry(JsckInfo info, ObjId id, SimpleField<?> subField, ComplexField<?> field,
      byte[] value, byte[] suffix) {
        this.verifySimpleIndexEntry(info, id, subField.getStorageId(), "" + subField.getIndex(), value, suffix);
    }

    private void verifySimpleIndexEntry(JsckInfo info, ObjId id, int storageId, String description, byte[] value, byte[] suffix) {

        // Build index entry
        final ByteWriter writer = new ByteWriter();
        Encodings.UNSIGNED_INT.write(writer, storageId);
        writer.write(value);
        id.writeTo(writer);
        writer.write(suffix);

        // Verify index entry
        this.verifyIndexEntry(info, id, writer.getBytes(), description);
    }

    /**
     * Verify a composite index entry.
     */
    private void verifyCompositeIndexEntry(JsckInfo info,
      ObjId id, CompositeIndex index, HashMap<String, byte[]> simpleFieldValues) {

        // Build index entry
        final ByteWriter writer = new ByteWriter();
        Encodings.UNSIGNED_INT.write(writer, index.getStorageId());
        for (SimpleField<?> field : index.getFields()) {

            // Get the field's value
            byte[] value = simpleFieldValues.get(field.getName());
            if (value == null)
                value = field.getEncoding().getDefaultValueBytes();

            // Append to index entry
            writer.write(value);
        }
        id.writeTo(writer);

        // Verify index entry
        this.verifyIndexEntry(info, id, writer.getBytes(), "" + index);
    }

    private void verifySchemaIndexEntry(JsckInfo info, ObjId id, int schemaIndex) {
        assert schemaIndex > 0;

        // Build index entry
        final ByteWriter writer = new ByteWriter();
        writer.write(Layout.getSchemaIndexKeyPrefix());
        Encodings.UNSIGNED_INT.write(writer, schemaIndex);
        id.writeTo(writer);

        // Verify index entry
        this.verifyIndexEntry(info, id, writer.getBytes(), "object schema index");
    }

    private void verifyIndexEntry(JsckInfo info, ObjId id, byte[] key, String description) {
        if (info.isDetailEnabled())
            info.detail("checking object %s %s entry", id, description);
        final byte[] value = info.getKVStore().get(key);
        if (value == null)
            info.handle(new MissingKey(String.format("missing index entry for %s", description), key, ByteUtil.EMPTY));
        else if (value.length != 0)
            info.handle(new InvalidValue(String.format("invalid non-empty value for %s", description), key, value, ByteUtil.EMPTY));
    }
}
