
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import com.google.common.base.Preconditions;

import io.permazen.core.InvalidSchemaException;
import io.permazen.util.DiffGenerating;
import io.permazen.util.Diffs;
import io.permazen.util.NavigableSets;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.function.Supplier;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.xml.IndentXMLStreamWriter;

/**
 * Models one JSimpleDB {@link io.permazen.core.Database} schema version.
 */
public class SchemaModel extends SchemaSupport implements DiffGenerating<SchemaModel> {

    static final Map<QName, Class<? extends SchemaField>> FIELD_TAG_MAP = new HashMap<>();
    static {
        FIELD_TAG_MAP.put(XMLConstants.COUNTER_FIELD_TAG, CounterSchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.ENUM_FIELD_TAG, EnumSchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.LIST_FIELD_TAG, ListSchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.MAP_FIELD_TAG, MapSchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.REFERENCE_FIELD_TAG, ReferenceSchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.SET_FIELD_TAG, SetSchemaField.class);
        FIELD_TAG_MAP.put(XMLConstants.SIMPLE_FIELD_TAG, SimpleSchemaField.class);
    }
    static final Map<QName, Class<? extends SimpleSchemaField>> SIMPLE_FIELD_TAG_MAP = new HashMap<>();
    static {
        SchemaModel.FIELD_TAG_MAP.entrySet().stream()
          .filter(entry -> SimpleSchemaField.class.isAssignableFrom(entry.getValue()))
          .forEach(entry -> SIMPLE_FIELD_TAG_MAP.put(entry.getKey(), entry.getValue().asSubclass(SimpleSchemaField.class)));
    }
    static final Map<QName, Class<? extends AbstractSchemaItem>> FIELD_OR_COMPOSITE_INDEX_TAG_MAP = new HashMap<>();
    static {
        FIELD_OR_COMPOSITE_INDEX_TAG_MAP.putAll(FIELD_TAG_MAP);
        FIELD_OR_COMPOSITE_INDEX_TAG_MAP.put(XMLConstants.COMPOSITE_INDEX_TAG, SchemaCompositeIndex.class);
    }

    private static final String XML_OUTPUT_FACTORY_PROPERTY = "javax.xml.stream.XMLOutputFactory";
    private static final String DEFAULT_XML_OUTPUT_FACTORY_IMPLEMENTATION = "com.sun.xml.internal.stream.XMLOutputFactoryImpl";

    private static final int CURRENT_FORMAT_VERSION = 2;

    // Keys into this.lockedDownCache
    private static final int VALIDATION_RESULT_KEY = 1;
    private static final int COMPATIBILITY_HASH_KEY = 2;

    private /*final*/ NavigableMap<Integer, SchemaObjectType> schemaObjectTypes = new TreeMap<>();

    // Computed information we cache after being locked down
    private final HashMap<Integer, Object> lockedDownCache = new HashMap<>();

    public NavigableMap<Integer, SchemaObjectType> getSchemaObjectTypes() {
        return this.schemaObjectTypes;
    }

    /**
     * Serialize an instance to the given XML output.
     *
     * <p>
     * The {@code output} is not closed by this method.
     *
     * @param output XML output
     * @param indent true to pretty print the XML
     * @throws IOException if an I/O error occurs
     */
    public void toXML(OutputStream output, boolean indent) throws IOException {
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
            if (indent)
                writer = new IndentXMLStreamWriter(writer);
            writer.writeStartDocument("UTF-8", "1.0");
            this.writeXML(writer);
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
     * Deserialize an instance from the given XML input and validate it.
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
            schemaModel.readXML(reader);
        } catch (XMLStreamException e) {
            throw new InvalidSchemaException("error parsing schema model XML", e);
        }
        schemaModel.validate();
        return schemaModel;
    }

// Lockdown

    @Override
    void lockDownRecurse() {
        super.lockDownRecurse();
        this.schemaObjectTypes = Collections.unmodifiableNavigableMap(this.schemaObjectTypes);
        for (SchemaObjectType schemaObjectType : this.schemaObjectTypes.values())
            schemaObjectType.lockDown();
    }

    private <T> T calculate(Class<T> type, int key, Supplier<T> supplier) {
        if (!this.lockedDown)
            return supplier.get();
        T value = type.cast(this.lockedDownCache.get(key));                 // can't use computeIfAbsent() because of null values
        if (value == null && !this.lockedDownCache.containsKey(key)) {
            value = supplier.get();
            this.lockedDownCache.put(key, value);
        }
        return value;
    }

// Validation

    /**
     * Validate this instance.
     *
     * <p>
     * This performs some basic structural validation. Full validation is not possible without a
     * {@link io.permazen.core.Database} instance (for example, we don't know whether or not a custom
     * {@link SimpleSchemaField} type name is registered with the associated {@link io.permazen.core.FieldTypeRegistry}).
     *
     * <p>
     * Note that after this instance has been {@linkplain #lockDown locked down}, repeated invocations of this
     * method will be very fast, just returning the cached previous result.
     *
     * @throws InvalidSchemaException if this instance is detected to be invalid
     */
    public void validate() {
        final RuntimeException failure = this.calculate(RuntimeException.class, VALIDATION_RESULT_KEY, () -> {
            try {
                this.doValidate();
            } catch (RuntimeException e) {
                return e;
            }
            return null;
        });
        if (failure != null)
            throw failure;
    }

    private void doValidate() {

        // Validate object types and verify object type names are unique
        final TreeMap<String, SchemaObjectType> schemaObjectTypesByName = new TreeMap<>();
        for (SchemaObjectType schemaObjectType : this.schemaObjectTypes.values()) {
            schemaObjectType.validate();
            final String schemaObjectTypeName = schemaObjectType.getName();
            final SchemaObjectType otherSchemaObjectType = schemaObjectTypesByName.put(schemaObjectTypeName, schemaObjectType);
            if (otherSchemaObjectType != null)
                throw new InvalidSchemaException("duplicate object name `" + schemaObjectTypeName + "'");
        }

        // Collect all field storage ID's
        final TreeMap<Integer, AbstractSchemaItem> globalItemsByStorageId = new TreeMap<>();
        for (SchemaObjectType schemaObjectType : this.schemaObjectTypes.values()) {
            for (SchemaField field : schemaObjectType.getSchemaFields().values()) {
                globalItemsByStorageId.put(field.getStorageId(), field);
                if (field instanceof ComplexSchemaField) {
                    final ComplexSchemaField complexField = (ComplexSchemaField)field;
                    for (SimpleSchemaField subField : complexField.getSubFields().values())
                        globalItemsByStorageId.put(subField.getStorageId(), subField);
                }
            }
        }

        // Verify object type, field, and index storage ID's are non-overlapping
        for (SchemaObjectType schemaObjectType : this.schemaObjectTypes.values()) {
            SchemaModel.verifyUniqueStorageId(globalItemsByStorageId, schemaObjectType);
            for (SchemaCompositeIndex index : schemaObjectType.getSchemaCompositeIndexes().values())
                SchemaModel.verifyUniqueStorageId(globalItemsByStorageId, index);
        }
    }

    static <T extends AbstractSchemaItem> void verifyUniqueStorageId(TreeMap<Integer, T> itemsByStorageId, T item) {
        final int storageId = item.getStorageId();
        final T previous = itemsByStorageId.get(storageId);
        if (previous != null && !previous.equals(item)) {
            throw new InvalidSchemaException("incompatible duplicate use of storage ID "
              + storageId + " by both " + previous + " and " + item);
        }
        itemsByStorageId.put(storageId, item);
    }

// Compatibility

    /**
     * Determine whether this instance is compatible with the given instance for use with the core API
     * and the same version number.
     *
     * <p>
     * Two instances are "same version" compatible if and only if they can be used to successfully
     * {@linkplain io.permazen.core.Database#createTransaction(SchemaModel, int, boolean) open a transaction}
     * against the same {@link io.permazen.core.Database} using the same schema version.
     *
     * <p>
     * Such compatibility implies the instances are mostly identical, with these notable exceptions:
     * <ul>
     *  <li>Object and field names (the core API identifies types and fields using only storage ID's)</li>
     *  <li>Attributes affecting behavior but not structure, such as
     *      {@linkplain ReferenceSchemaField#isCascadeDelete delete cascades}</li>
     * </ul>
     *
     * <p>
     * To determine whether two instances are truly identical, use {@link #equals equals()}.
     *
     * @param that other schema model
     * @return true if this and {@code that} are "same version" compatible
     * @throws InvalidSchemaException if either this or {@code that} instance is invalid
     * @throws IllegalArgumentException if {@code that} is null
     */
    public boolean isCompatibleWith(SchemaModel that) {
        Preconditions.checkArgument(that != null, "null that");
        if (this == that)
            return true;
        return AbstractSchemaItem.isAll(this.schemaObjectTypes, that.schemaObjectTypes, SchemaObjectType::isCompatibleWith);
    }

    /**
     * Generate a "same version" compatibility hash value.
     *
     * <p>
     * For any two instances, if they are {@linkplain #isCompatibleWith "same version" compatible} then
     * the returned value will be the same; if they are different, the returned value is very likely to be different.
     *
     * @return "same version" compatibility hash value
     */
    public long compatibilityHash() {
        return this.calculate(Long.class, COMPATIBILITY_HASH_KEY, this::computeCompatibilityHash);
    }

    private long computeCompatibilityHash() {
        final MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        final OutputStream discardOutputStream = new OutputStream() {
            @Override
            public void write(int b) {
            }
            @Override
            public void write(byte[] b, int off, int len) {
            }
        };
        try (DigestOutputStream output = new DigestOutputStream(discardOutputStream, sha1)) {
            this.writeCompatibilityHashData(output);
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(sha1.digest()))) {
            return input.readLong();
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }
    }

    private void writeCompatibilityHashData(OutputStream stream) throws IOException {
        try (DataOutputStream output = new DataOutputStream(stream)) {
            output.writeInt(this.schemaObjectTypes.size());
            for (SchemaObjectType schemaObjectType : this.schemaObjectTypes.values())
                schemaObjectType.writeCompatibilityHashData(output);
        }
    }

    /**
     * Auto-generate a random schema version based on this instance's
     * {@linkplain #compatibilityHash "same version" compatibility hash value}.
     *
     * @return schema version number, always greater than zero
     */
    public int autogenerateVersion() {
        int version = (int)(this.compatibilityHash() >>> 33);                       // ensure value is non-negative
        if (version == 0)                                                           // handle unlikely zero case
            version = 1;
        return version;
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SchemaModel that) {
        Preconditions.checkArgument(that != null, "null that");
        final Diffs diffs = new Diffs();
        final NavigableSet<Integer> allObjectTypeIds = NavigableSets.union(
          this.schemaObjectTypes.navigableKeySet(), that.schemaObjectTypes.navigableKeySet());
        for (int storageId : allObjectTypeIds) {
            final SchemaObjectType thisObjectType = this.schemaObjectTypes.get(storageId);
            final SchemaObjectType thatObjectType = that.schemaObjectTypes.get(storageId);
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

    void readXML(XMLStreamReader reader) throws XMLStreamException {
        this.schemaObjectTypes.clear();

        // Read opening tag
        this.expect(reader, false, XMLConstants.SCHEMA_MODEL_TAG);

        // Get and verify format version
        final Integer formatAttr = this.getIntAttr(reader, XMLConstants.FORMAT_VERSION_ATTRIBUTE, false);
        final int formatVersion = formatAttr != null ? formatAttr : 0;
        final QName objectTypeTag;
        switch (formatVersion) {
        case 0:
            objectTypeTag = new QName("Object");
            break;
        case 1:                                             // changed <Object> to <ObjectType>
        case 2:                                             // added <CompositeIndex>
            objectTypeTag = XMLConstants.OBJECT_TYPE_TAG;
            break;
        default:
            throw new XMLStreamException("unrecognized schema format version " + formatAttr, reader.getLocation());
        }

        // Read object type tags
        while (this.expect(reader, true, objectTypeTag)) {
            final SchemaObjectType schemaObjectType = new SchemaObjectType();
            schemaObjectType.readXML(reader, formatVersion);
            final int storageId = schemaObjectType.getStorageId();
            final SchemaObjectType previous = this.schemaObjectTypes.put(storageId, schemaObjectType);
            if (previous != null) {
                throw new XMLStreamException("duplicate use of storage ID " + storageId
                  + " for both " + previous + " and " + schemaObjectType, reader.getLocation());
            }
        }
    }

// XML Writing

    void writeXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.setDefaultNamespace(XMLConstants.SCHEMA_MODEL_TAG.getNamespaceURI());
        writer.writeStartElement(XMLConstants.SCHEMA_MODEL_TAG.getNamespaceURI(), XMLConstants.SCHEMA_MODEL_TAG.getLocalPart());
        writer.writeAttribute(XMLConstants.FORMAT_VERSION_ATTRIBUTE.getNamespaceURI(),
          XMLConstants.FORMAT_VERSION_ATTRIBUTE.getLocalPart(), "" + CURRENT_FORMAT_VERSION);
        final ArrayList<SchemaObjectType> typeList = new ArrayList<>(this.schemaObjectTypes.values());
        Collections.sort(typeList, Comparator.comparing(SchemaObjectType::getName));
        for (SchemaObjectType schemaObjectType : typeList)
            schemaObjectType.writeXML(writer);
        writer.writeEndElement();
    }

// Object

    @Override
    public String toString() {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            this.toXML(buf, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new String(buf.toByteArray(), Charset.forName("UTF-8"))
          .replaceAll("(?s)<\\?xml version=\"1\\.0\" encoding=\"UTF-8\"\\?>\n", "").trim();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final SchemaModel that = (SchemaModel)obj;
        return this.schemaObjectTypes.equals(that.schemaObjectTypes);
    }

    @Override
    public int hashCode() {
        return this.schemaObjectTypes.hashCode();
    }

// Cloneable

    /**
     * Deep-clone this instance.
     */
    @Override
    @SuppressWarnings("unchecked")
    public SchemaModel clone() {
        final SchemaModel clone = (SchemaModel)super.clone();
        clone.schemaObjectTypes = new TreeMap<>(clone.schemaObjectTypes);
        for (Map.Entry<Integer, SchemaObjectType> entry : clone.schemaObjectTypes.entrySet())
            entry.setValue(entry.getValue().clone());
        return clone;
    }
}

