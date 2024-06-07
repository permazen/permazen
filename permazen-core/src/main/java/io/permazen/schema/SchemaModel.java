
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import com.google.common.base.Preconditions;

import io.permazen.core.Database;
import io.permazen.core.InvalidSchemaException;
import io.permazen.encoding.EncodingId;
import io.permazen.encoding.EncodingRegistry;
import io.permazen.tuple.Tuple2;
import io.permazen.util.DiffGenerating;
import io.permazen.util.Diffs;
import io.permazen.util.NavigableSets;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.function.Consumer;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.xml.IndentXMLStreamWriter;

/**
 * Models one Permazen {@link Database} schema version.
 */
public class SchemaModel extends SchemaSupport implements DiffGenerating<SchemaModel> {

    /**
     * The {@link ItemType} that this class represents.
     */
    public static final ItemType ITEM_TYPE = ItemType.SCHEMA_MODEL;

    static final Map<QName, Class<? extends SchemaField>> FIELD_TAG_MAP = new HashMap<>();
    static {
        FIELD_TAG_MAP.put(XMLConstants.COUNTER_FIELD_TAG,       CounterSchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.ENUM_FIELD_TAG,          EnumSchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.ENUM_ARRAY_FIELD_TAG,    EnumArraySchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.LIST_FIELD_TAG,          ListSchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.MAP_FIELD_TAG,           MapSchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.REFERENCE_FIELD_TAG,     ReferenceSchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.SET_FIELD_TAG,           SetSchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.SIMPLE_FIELD_TAG,        SimpleSchemaField.class);
    }
    static final Map<QName, Class<? extends SimpleSchemaField>> SIMPLE_FIELD_TAG_MAP = new HashMap<>();
    static {
        SchemaModel.FIELD_TAG_MAP.entrySet().stream()
          .filter(entry -> SimpleSchemaField.class.isAssignableFrom(entry.getValue()))
          .forEach(entry -> SIMPLE_FIELD_TAG_MAP.put(entry.getKey(), entry.getValue().asSubclass(SimpleSchemaField.class)));
    }
    static final Map<QName, Class<? extends SchemaItem>> FIELD_OR_COMPOSITE_INDEX_TAG_MAP = new HashMap<>();
    static {
        FIELD_OR_COMPOSITE_INDEX_TAG_MAP.putAll(FIELD_TAG_MAP);
        FIELD_OR_COMPOSITE_INDEX_TAG_MAP.put(XMLConstants.COMPOSITE_INDEX_TAG, SchemaCompositeIndex.class);
    }

    private static final RuntimeException VALIDATION_OK = new RuntimeException();   // validation sentinel value

    private static final String XML_OUTPUT_FACTORY_PROPERTY = "javax.xml.stream.XMLOutputFactory";
    private static final String DEFAULT_XML_OUTPUT_FACTORY_IMPLEMENTATION = "com.sun.xml.internal.stream.XMLOutputFactoryImpl";

    // Current format version for schema XML
    private static final int CURRENT_FORMAT_VERSION = 0;

    private NavigableMap<String, SchemaObjectType> objectTypes = new TreeMap<>();

    // Cached info (after full lockdown only)
    private RuntimeException validation;

// Properties

    /**
     * Get the object types defined in this schema.
     *
     * @return object types keyed by name
     */
    public NavigableMap<String, SchemaObjectType> getSchemaObjectTypes() {
        return this.objectTypes;
    }

    /**
     * Determine if this schema is empty, i.e., defines zero object types.
     *
     * @return true if this is an empty schema
     */
    public boolean isEmpty() {
        return this.objectTypes.isEmpty();
    }

// Recursion

    @Override
    public void visitSchemaItems(Consumer<? super SchemaItem> visitor) {
        super.visitSchemaItems(visitor);
        this.objectTypes.values().forEach(objType -> objType.visitSchemaItems(visitor));
    }

// Lockdown

    /**
     * Lock down this {@link SchemaModel} and every {@link SchemaItem} it contains so that no further changes can be made.
     *
     * <p>
     * There are two levels of lock down: the first level locks everything, except storage ID's that are zero
     * may be changed to a non-zero value. The second level locks down everything. Levels increase monotonically.
     *
     * <p>
     * Attempts to modify a locked down schema item generate an {@link UnsupportedOperationException}.
     *
     * @param includingStorageIds false to exclude storage ID's, true to lock down everything
     */
    public void lockDown(boolean includingStorageIds) {
        if (!this.lockedDown1)
            this.lockDown1();
        if (includingStorageIds && !this.lockedDown2)
            this.lockDown2();
    }

    @Override
    void lockDown1() {
        super.lockDown1();
        this.objectTypes = this.lockDownMap1(this.objectTypes);
    }

    @Override
    void lockDown2() {
        super.lockDown2();
        this.objectTypes.values().forEach(SchemaObjectType::lockDown2);
    }

// XML Serialization

    /**
     * Serialize this instance to the given XML output.
     *
     * <p>
     * The {@code output} is not closed by this method.
     *
     * @param output XML output
     * @param includeStorageIds true to include storage ID's
     * @param prettyPrint true to indent the XML and add schema ID comments
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if {@code output} is null
     */
    public void toXML(OutputStream output, boolean includeStorageIds, boolean prettyPrint) throws IOException {
        Preconditions.checkArgument(output != null, "null output");
        try {

            // Create factory, preferring Sun implementation to avoid https://github.com/FasterXML/woodstox/issues/17
            XMLOutputFactory factory;
            final boolean setDefault = System.getProperty(XML_OUTPUT_FACTORY_PROPERTY) == null;
            if (setDefault)
                System.setProperty(XML_OUTPUT_FACTORY_PROPERTY, DEFAULT_XML_OUTPUT_FACTORY_IMPLEMENTATION);
            try {
                factory = XMLOutputFactory.newInstance();
            } catch (RuntimeException e) {
                if (!setDefault)
                    throw e;
                System.clearProperty(XML_OUTPUT_FACTORY_PROPERTY);
                factory = XMLOutputFactory.newInstance();
            }

            // Create writer
            XMLStreamWriter writer = factory.createXMLStreamWriter(output, "UTF-8");
            if (prettyPrint)
                writer = new IndentXMLStreamWriter(writer);
            writer.writeStartDocument("UTF-8", "1.0");
            this.writeXML(writer, includeStorageIds, prettyPrint);
            writer.writeEndDocument();
            writer.flush();
        } catch (XMLStreamException e) {
            if (e.getCause() instanceof IOException)
                throw (IOException)e.getCause();
            throw new RuntimeException("internal error", e);
        }

        // Output final newline
        new PrintStream(output, true, "UTF-8").println();
        output.flush();
    }

    /**
     * Deserialize an instance from the given XML input.
     *
     * @param input XML input
     * @return deserialized schema model
     * @throws IOException if an I/O error occurs
     * @throws InvalidSchemaException if the XML input or decoded {@link SchemaModel} is invalid
     * @throws IllegalArgumentException if {@code input} is null
     */
    public static SchemaModel fromXML(InputStream input) throws IOException {
        Preconditions.checkArgument(input != null, "null input");
        final SchemaModel schemaModel = new SchemaModel();
        try {
            final XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(input);
            schemaModel.readXML(reader, false);
        } catch (XMLStreamException e) {
            if (e.getCause() instanceof IOException)        // XMLInputFactory API should throw IOException directly
                throw (IOException)e.getCause();
            throw new InvalidSchemaException(String.format("error in schema XML: %s", e.getMessage()), e);
        }
        return schemaModel;
    }

// Validation

    /**
     * Validate this instance.
     *
     * <p>
     * This performs basic structural validation. Full validation is not possible without a
     * {@link Database} instance; for example, simple field {@link EncodingId}'s must be resolved
     * in the {@linkplain Database#getEncodingRegistry associated} {@link EncodingRegistry}.
     *
     * <p>
     * Once this instance is {@linkplain #lockDown locked down}, repeated invocations of this method will be very fast,
     * just returning the cached previous result.
     *
     * @throws InvalidSchemaException if this instance is invalid
     * @throws IllegalStateException if this instance is not locked down
     */
    public final void validate() {

        // Sanity check
        Preconditions.checkArgument(this.lockedDown1, "not locked down");

        // Compute validation status if not already cached
        RuntimeException result;
        if (this.validation != null)
            result = this.validation;
        else {

            // Calculate whether valid
            try {
                this.doValidate();
                result = VALIDATION_OK;
            } catch (RuntimeException e) {
                result = e;
            }

            // Cache value if fully locked down
            if (this.lockedDown2)
                this.validation = result;
        }

        // Check result
        if (result != VALIDATION_OK)
            throw result;
    }

    void doValidate() {

        // Verify mapped object type names
        this.verifyMappedNames("object type", this.objectTypes);

        // Validate object types
        this.objectTypes.values().forEach(SchemaObjectType::validate);

        // Verify reference field object type restrictions
        this.visitSchemaItems(ReferenceSchemaField.class, field -> {
            final NavigableSet<String> refObjectTypes = field.getObjectTypes();
            if (refObjectTypes == null)
                return;
            for (String typeName : refObjectTypes) {
                if (!this.objectTypes.containsKey(typeName))
                    throw new InvalidSchemaException(String.format("invalid %s: unknown type restriction \"%s\"", field, typeName));
            }
        });

        // Verify that non-zero storage ID's don't have any conflicts or duplicates
        final HashMap<Integer, Tuple2<SchemaId, SchemaItem>> prevSchemaIdMap = new HashMap<>();
        final HashMap<SchemaId, Tuple2<Integer, SchemaItem>> prevStorageIdMap = new HashMap<>();
        this.visitSchemaItems(item -> {
            final int storageId = item.getStorageId();
            if (storageId <= 0)
                return;
            final SchemaId schemaId = item.getSchemaId();
            final Tuple2<SchemaId, SchemaItem> prevSchemaId = prevSchemaIdMap.put(storageId, new Tuple2<>(schemaId, item));
            if (prevSchemaId != null && !prevSchemaId.getValue1().equals(schemaId)) {
                throw new InvalidSchemaException(
                  String.format("conflicting assignment of storage ID %d to both %s and %s",
                  storageId, prevSchemaId.getValue2(), item));
            }
            final Tuple2<Integer, SchemaItem> prevStorageId = prevStorageIdMap.put(schemaId, new Tuple2<>(storageId, item));
            if (prevStorageId != null && !prevStorageId.getValue1().equals(storageId)) {
                throw new InvalidSchemaException(
                  String.format("duplicate assignment of storage IDs %d and %d to storage ID \"%s\" (%s)",
                  prevStorageId.getValue1(), storageId, schemaId, item));
            }
        });
    }

    /**
     * {@linkplain SchemaModel#validate Validate} this instance itself and also verify that all of its
     * {@linkplain SimpleSchemaField#getEncodingId field encodings} can be found in the given {@link EncodingRegistry}.
     *
     * @param encodingRegistry registry of encodings
     * @throws InvalidSchemaException if this instance is itself invalid
     * @throws InvalidSchemaException if any simple field's encoding ID can't be resovled by {@code encodingRegistry}
     * @throws IllegalStateException if this instance is not locked down
     * @throws IllegalArgumentException if {@code encodingRegistry} is null
     */
    public void validateWithEncodings(EncodingRegistry encodingRegistry) {

        // Sanity check
        Preconditions.checkArgument(encodingRegistry != null, "null encodingRegistry");

        // Validate normally
        this.validate();

        // Validate simple field encodings
        this.visitSchemaItems(SimpleSchemaField.class, field -> {
            if (field.hasFixedEncoding())
                return;
            final EncodingId encodingId = field.getEncodingId();
            if (encodingRegistry.getEncoding(encodingId) == null) {
                throw new InvalidSchemaException(
                  String.format("unknown encoding \"%s\" for field \"%s\"", encodingId, field.getName()));
            }
        });
    }

// Storage ID's

    /**
     * Reset all non-zero storage ID's in this instance back to zero.
     *
     * @throws UnsupportedOperationException if this instance is {@linkplain #lockDown fully locked down}
     */
    public void resetStorageIds() {
        this.visitSchemaItems(item -> item.setStorageId(0));
    }

// Schema ID

    @Override
    public final ItemType getItemType() {
        return ITEM_TYPE;
    }

    @Override
    void writeSchemaIdHashData(DataOutputStream output) throws IOException {
        super.writeSchemaIdHashData(output);
        output.writeInt(this.objectTypes.size());
        for (SchemaObjectType objectType : this.objectTypes.values())
            objectType.writeSchemaIdHashData(output, true);
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SchemaModel that) {
        Preconditions.checkArgument(that != null, "null that");
        final Diffs diffs = new Diffs();
        final NavigableSet<String> allObjectTypeNames = NavigableSets.union(
          this.getSchemaObjectTypes().navigableKeySet(), that.getSchemaObjectTypes().navigableKeySet());
        for (String typeName : allObjectTypeNames) {
            final SchemaObjectType thisObjectType = this.getSchemaObjectTypes().get(typeName);
            final SchemaObjectType thatObjectType = that.getSchemaObjectTypes().get(typeName);
            if (thisObjectType == null)
                diffs.add("removed " + thatObjectType);
            else if (thatObjectType == null)
                diffs.add("added " + thisObjectType);
            else {
                final Diffs objectTypeDiffs = thisObjectType.differencesFrom(thatObjectType);
                if (!objectTypeDiffs.isEmpty())
                    diffs.add("changed " + thatObjectType, objectTypeDiffs);
            }
        }
        return diffs;
    }

// XML Reading

    /**
     * Reset this instance and (re)populate it from the given XML input.
     *
     * @param reader XML input
     * @param openTag true if the opening XML {@code <SchemaModel>} tag has already been read from {@code input},
     *  false to expect to read the tag as the next XML element
     * @throws XMLStreamException if the XML input is invalid
     * @throws IllegalArgumentException if {@code reader} is null
     */
    public void readXML(XMLStreamReader reader, boolean openTag) throws XMLStreamException {

        // Sanity check
        Preconditions.checkArgument(reader != null, "null reader");

        // Reset state
        this.objectTypes.clear();

        // Read opening tag if needed
        if (!openTag)
            this.expect(reader, false, XMLConstants.SCHEMA_MODEL_TAG);

        // Get and verify format version
        final Integer formatAttr = this.getIntAttr(reader, XMLConstants.FORMAT_VERSION_ATTRIBUTE, false);
        final int formatVersion = formatAttr != null ? formatAttr : 0;
        final QName objectTypeTag;
        switch (formatVersion) {
        case 0:
            break;
        default:
            throw this.newInvalidInputException(reader, "unrecognized schema XML format version %d", formatAttr);
        }

        // Read object type tags
        while (this.expect(reader, true, XMLConstants.OBJECT_TYPE_TAG)) {
            final SchemaObjectType objectType = new SchemaObjectType();
            objectType.readXML(reader, formatVersion);
            final String typeName = objectType.getName();
            if (this.objectTypes.put(typeName, objectType) != null)
                throw this.newInvalidInputException(reader, "duplicate use of object name \"%s\"", typeName);
        }
    }

// XML Writing

    /**
     * Write this instance to the given XML output.
     *
     * @param writer XML output
     * @param includeStorageIds true to include storage ID's
     * @param prettyPrint true to indent and include {@link SchemaId} comments
     * @throws XMLStreamException if an XML error occurs
     * @throws IllegalArgumentException if {@code writer} is null
     */
    public void writeXML(XMLStreamWriter writer, boolean includeStorageIds, boolean prettyPrint) throws XMLStreamException {

        // Sanity check
        Preconditions.checkArgument(writer != null, "null writer");

        // Get format version
        assert CURRENT_FORMAT_VERSION == 0;
        final int formatVersion = CURRENT_FORMAT_VERSION;

        // Output XML
        writer.setDefaultNamespace(XMLConstants.SCHEMA_MODEL_TAG.getNamespaceURI());
        this.writeStartElement(writer, XMLConstants.SCHEMA_MODEL_TAG);
        if (formatVersion != 0)
            this.writeAttr(writer, XMLConstants.FORMAT_VERSION_ATTRIBUTE, formatVersion);
        if (prettyPrint)
            this.writeSchemaIdComment(writer);
        for (SchemaObjectType objectType : this.objectTypes.values())
            objectType.writeXML(writer, includeStorageIds, prettyPrint);
        writer.writeEndElement();
    }

// Object

    /**
     * Returns this schema model in XML form.
     */
    @Override
    public String toString() {
        return this.toString(false, true);
    }

    /**
     * Returns this schema model in XML form.
     *
     * @param writer XML output
     * @param includeStorageIds true to include storage ID's
     * @param prettyPrint true to indent and include {@link SchemaId} comments
     */
    public String toString(boolean includeStorageIds, boolean prettyPrint) {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            this.toXML(buf, includeStorageIds, prettyPrint);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new String(buf.toByteArray(), StandardCharsets.UTF_8)
          .replaceAll("^<\\?xml[^>]+\\?>\\n", "").trim();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final SchemaModel that = (SchemaModel)obj;
        return this.objectTypes.equals(that.objectTypes);
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ this.objectTypes.hashCode();
    }

// Cloneable

    /**
     * Deep-clone this instance.
     */
    @Override
    @SuppressWarnings("unchecked")
    public SchemaModel clone() {
        final SchemaModel clone = (SchemaModel)super.clone();
        clone.objectTypes = this.cloneMap(clone.objectTypes);
        clone.validation = null;
        return clone;
    }
}
