
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
import io.permazen.util.ByteData;

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
        final ByteData objectPrefix = id.getBytes();

        // Keep track of which simple fields we see with non-default values
        final HashSet<SimpleField<?>> indexedSimpleFieldsWithDefaultValues = new HashSet<>(this.indexedSimpleFields);

        // Keep track of simple field values (after possible fixups)
        final HashMap<String, ByteData> simpleFieldValues = new HashMap<>();

        // Scan field data; note we will not see simple fields with default values in this loop
        while (i.hasNext()) {
            final KVPair pair = i.peek();
            final ByteData key = pair.getKey();

            // Have we reached the end of the object?
            if (!key.startsWith(objectPrefix))
                break;
            assert key.size() > ObjId.NUM_BYTES;

            // Decode field storage ID
            final ByteData.Reader keyReader = key.newReader(ObjId.NUM_BYTES);
            final int storageId;
            try {
                storageId = Encodings.UNSIGNED_INT.read(keyReader);
            } catch (IllegalArgumentException e) {
                info.handle(new InvalidKey(pair).setDetail(id, "invalid field storage ID"));
                continue;
            }
            if (storageId <= 0) {
                info.handle(new InvalidKey(pair).setDetail(id, "invalid field storage ID %d", storageId));
                continue;
            }

            // Find the field
            final Field<?> field;
            try {
                field = this.schemaItem.getField(storageId);
            } catch (UnknownFieldException e) {
                info.handle(new InvalidKey(pair).setDetail(id,
                  "invalid field storage ID %d: no such field exists in %s", storageId, this.schemaItem));
                continue;
            }

            // Build field prefix
            final ByteData fieldPrefix = keyReader.dataReadSoFar();

            // Scan field
            if (info.isDetailEnabled())
                info.detail("checking object %s %s", id, field);
            field.visit(new FieldSwitch<Void>() {
                @Override
                public <T> Void caseSimpleField(SimpleField<T> field) {
                    final ByteData value = ObjectType.this.checkSimpleField(info, id, field, fieldPrefix, i);
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
        assert !i.hasNext() || !i.peek().getKey().startsWith(objectPrefix);

        // Verify index entries for indexed simple fields that had default values (which we would not have encountered)
        for (SimpleField<?> field : indexedSimpleFieldsWithDefaultValues) {
            final Encoding<?> encoding = field.getEncoding();
            final ByteData defaultValue = encoding.getDefaultValueBytes();
            this.verifySimpleIndexEntry(info, id, field, defaultValue);
        }

        // Verify composite index entries
        for (CompositeIndex index : this.schemaItem.getCompositeIndexes().values())
            this.verifyCompositeIndexEntry(info, id, index, simpleFieldValues);

        // Verify object schema index entry
        this.verifySchemaIndexEntry(info, id, schemaIndex);
    }

    // Returns field's binary value if field has non-default value, otherwise null
    private <T> ByteData checkSimpleField(JsckInfo info,
      ObjId id, SimpleField<T> field, ByteData prefix, PeekingIterator<KVPair> i) {

        // Get encoding
        final Encoding<?> encoding = field.getEncoding();
        assert encoding != null;

        // Get field key/value pair
        final KVPair pair = i.next();
        assert pair != null;
        assert pair.getKey().startsWith(prefix);

        // Check for trailing garbage in key
        if (pair.getKey().size() > prefix.size()) {
            info.handle(new InvalidKey(pair).setDetail(id, field,
              "trailing garbage %s", Jsck.ds(pair.getKey().newReader(prefix.size()))));
            return null;
        }

        // Decode value
        ByteData value = pair.getValue();
        final ByteData.Reader reader = pair.getValue().newReader();
        if (!this.validateSimpleFieldValue(info, id, field, pair, reader))
            value = null;

        // We should not see default values in simple fields that are not sub-fields of complex fields
        if (value != null && value.compareTo(encoding.getDefaultValueBytes()) == 0) {
            info.handle(new InvalidValue(pair).setDetail("default value should not be present"));
            value = null;
        }

        // Verify index entry
        if (field.isIndexed())
            this.verifySimpleIndexEntry(info, id, field, value != null ? value : encoding.getDefaultValueBytes());

        // Done
        return value;
    }

    private <E> void checkSetField(JsckInfo info, ObjId id, SetField<E> field, ByteData prefix, PeekingIterator<KVPair> i) {

        // Get element field
        final SimpleField<E> elementField = field.getElementField();

        // Iterate over set elements
        while (i.hasNext() && i.peek().getKey().startsWith(prefix)) {
            final KVPair pair = i.next();

            // Verify encoded element
            final ByteData.Reader reader = pair.getKey().newReader(prefix.size());
            if (!this.validateSimpleFieldValue(info, id, elementField, pair, reader))
                continue;

            // Value should be empty
            if (!pair.getValue().isEmpty())
                info.handle(new InvalidValue(pair, ByteData.empty()).setDetail(id, elementField, "should be empty"));

            // Verify index entry
            if (elementField.isIndexed()) {
                this.verifySimpleIndexEntry(info, id, elementField,
                  field, reader.getByteData().substring(prefix.size()), ByteData.empty());
            }
        }
    }

    private <K, V> void checkMapField(JsckInfo info, ObjId id, MapField<K, V> field, ByteData prefix, PeekingIterator<KVPair> i) {

        // Get key and value fields
        final SimpleField<K> keyField = field.getKeyField();
        final SimpleField<V> valField = field.getValueField();

        // Iterate over set elements
        while (i.hasNext() && i.peek().getKey().startsWith(prefix)) {
            final KVPair pair = i.next();

            // Verify encoded key
            final ByteData.Reader keyReader = pair.getKey().newReader(prefix.size());
            if (!this.validateSimpleFieldValue(info, id, keyField, pair, keyReader))
                continue;

            // Verify encoded value
            final ByteData.Reader valReader = pair.getValue().newReader();
            if (!this.validateSimpleFieldValue(info, id, valField, pair, valReader))
                continue;

            // Verify index entries
            if (keyField.isIndexed()) {
                this.verifySimpleIndexEntry(info, id, keyField,
                  field, keyReader.getByteData().substring(prefix.size()), ByteData.empty());
            }
            if (valField.isIndexed()) {
                this.verifySimpleIndexEntry(info, id, valField,
                  field, pair.getValue(), keyReader.getByteData().substring(prefix.size()));
            }
        }
    }

    private <E> void checkListField(JsckInfo info, ObjId id, ListField<E> field, ByteData prefix, PeekingIterator<KVPair> i) {

        // Get element field and type
        final SimpleField<E> elementField = field.getElementField();

        // Iterate over list elements
        int expectedIndex = 0;
        while (i.hasNext() && i.peek().getKey().startsWith(prefix)) {
            final KVPair pair = i.next();

            // Decode list index
            final ByteData.Reader keyReader = pair.getKey().newReader(prefix.size());
            final int actualIndex;
            try {
                try {
                    actualIndex = Encodings.UNSIGNED_INT.read(keyReader);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(String.format("invalid list index: %s", e.getMessage()), e);
                }
                if (keyReader.remain() > 0) {
                    throw new IllegalArgumentException(String.format(
                      "trailing garbage %s after encoded index %d",
                      Jsck.ds(keyReader, keyReader.getOffset()), actualIndex));
                }
            } catch (IllegalArgumentException e) {
                info.handle(new InvalidValue(pair).setDetail(id, elementField, "%s", e.getMessage()));
                continue;
            }

            // Verify encoded element
            final ByteData.Reader valReader = pair.getValue().newReader();
            if (!this.validateSimpleFieldValue(info, id, elementField, pair, valReader))
                continue;

            // Check list index, and renumber if necessary
            ByteData encodedIndex = keyReader.getByteData().substring(prefix.size());
            if (actualIndex != expectedIndex) {
                info.handle(new InvalidValue(pair).setDetail(id, elementField, "wrong index %d != %d", actualIndex, expectedIndex));
                final ByteData.Writer keyWriter = ByteData.newWriter();
                keyWriter.write(prefix);
                Encodings.UNSIGNED_INT.write(keyWriter, expectedIndex);
                encodedIndex = keyWriter.toByteData().substring(prefix.size());
                info.handle(new MissingKey("incorrect list index", keyWriter.toByteData(), pair.getValue())
                  .setDetail(id, elementField, "renumbered list index %d -> %d", actualIndex, expectedIndex));
            }

            // Entry is good - we can advance the list index
            expectedIndex++;

            // Verify index entry
            if (elementField.isIndexed())
                this.verifySimpleIndexEntry(info, id, elementField, field, pair.getValue(), encodedIndex);
        }
    }

    private void checkCounterField(JsckInfo info, ObjId id, CounterField field, ByteData prefix, PeekingIterator<KVPair> i) {

        // Get field key/value pair
        final KVPair pair = i.next();
        assert pair != null;
        assert pair.getKey().startsWith(prefix);

        // Check for trailing garbage in key
        if (pair.getKey().size() > prefix.size()) {
            info.handle(new InvalidKey(pair).setDetail(id, field,
              "trailing garbage %s", Jsck.ds(pair.getKey().newReader(prefix.size()))));
            return;
        }

        // Decode value
        try {
            info.getKVStore().decodeCounter(pair.getValue());
        } catch (IllegalArgumentException e) {
            info.handle(new InvalidValue(pair).setDetail(id, field, "(resetting to zero): %s", e.getMessage()));
        }
    }

    private <T> boolean validateSimpleFieldValue(JsckInfo info,
      ObjId id, SimpleField<T> field, KVPair pair, ByteData.Reader reader) {

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
                        throw new IllegalArgumentException(String.format("invalid reference to deleted object %s", target));
                }
            }
        } catch (IllegalArgumentException e) {
            info.handle(new InvalidValue(pair).setDetail(id, field, "%s", e.getMessage()));
            return false;
        }
        return true;
    }

    private <T> void verifySimpleIndexEntry(JsckInfo info, ObjId id, SimpleField<T> field, ByteData value) {
        this.verifySimpleIndexEntry(info, id, field.getStorageId(), "" + field.getIndex(), value, ByteData.empty());
    }

    private void verifySimpleIndexEntry(JsckInfo info, ObjId id, SimpleField<?> subField, ComplexField<?> field,
      ByteData value, ByteData suffix) {
        this.verifySimpleIndexEntry(info, id, subField.getStorageId(), "" + subField.getIndex(), value, suffix);
    }

    private void verifySimpleIndexEntry(JsckInfo info, ObjId id,
      int storageId, String description, ByteData value, ByteData suffix) {

        // Build index entry
        final ByteData.Writer writer = ByteData.newWriter();
        Encodings.UNSIGNED_INT.write(writer, storageId);
        writer.write(value);
        id.writeTo(writer);
        writer.write(suffix);

        // Verify index entry
        this.verifyIndexEntry(info, id, writer.toByteData(), description);
    }

    /**
     * Verify a composite index entry.
     */
    private void verifyCompositeIndexEntry(JsckInfo info,
      ObjId id, CompositeIndex index, HashMap<String, ByteData> simpleFieldValues) {

        // Build index entry
        final ByteData.Writer writer = ByteData.newWriter();
        Encodings.UNSIGNED_INT.write(writer, index.getStorageId());
        for (SimpleField<?> field : index.getFields()) {

            // Get the field's value
            ByteData value = simpleFieldValues.get(field.getName());
            if (value == null)
                value = field.getEncoding().getDefaultValueBytes();

            // Append to index entry
            writer.write(value);
        }
        id.writeTo(writer);

        // Verify index entry
        this.verifyIndexEntry(info, id, writer.toByteData(), "" + index);
    }

    private void verifySchemaIndexEntry(JsckInfo info, ObjId id, int schemaIndex) {
        assert schemaIndex > 0;

        // Build index entry
        final ByteData.Writer writer = ByteData.newWriter();
        writer.write(Layout.getSchemaIndexKeyPrefix());
        Encodings.UNSIGNED_INT.write(writer, schemaIndex);
        id.writeTo(writer);

        // Verify index entry
        this.verifyIndexEntry(info, id, writer.toByteData(), "object schema index");
    }

    private void verifyIndexEntry(JsckInfo info, ObjId id, ByteData key, String description) {
        if (info.isDetailEnabled())
            info.detail("checking object %s %s entry", id, description);
        final ByteData value = info.getKVStore().get(key);
        if (value == null)
            info.handle(new MissingKey(String.format("missing index entry for %s", description), key, ByteData.empty()));
        else if (!value.isEmpty()) {
            info.handle(new InvalidValue(String.format(
              "invalid non-empty value for %s", description), key, value, ByteData.empty()));
        }
    }
}
