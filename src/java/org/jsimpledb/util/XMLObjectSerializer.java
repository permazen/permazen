
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import com.google.common.collect.Iterators;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.string.ParseContext;
import org.dellroad.stuff.xml.IndentXMLStreamWriter;
import org.jsimpledb.core.CollectionField;
import org.jsimpledb.core.CounterField;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ListField;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.ReferenceField;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.core.SetField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaField;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.schema.SchemaObject;

/**
 * Utility methods for serializing and deserializing objects in a {@link Transaction} to/from XML.
 *
 * <p>
 * There are two supported formats. The "Storage ID Format" specifies objects and fields using their storage IDs, and is robust:
 *  <pre>
 *  &lt;objects&gt;
 *      &lt;object storageId="100" id="c8a971e1aef01cc8" version="1"&gt;
 *          &lt;field storageId="101"&gt;George Washington&lt;/field&gt;
 *          &lt;field storageId="102"&gt;true&lt;/field&gt;
 *          &lt;field storageId="103"&gt;
 *              &lt;entry&gt;
 *                  &lt;key&gt;teeth&lt;/key&gt;
 *                  &lt;value&gt;wooden&lt;/value&gt;
 *              &lt;/entry&gt;
 *          &lt;/field&gt;
 *          &lt;field storageId="104"&gt;c8b84a08e5c2b1a2&lt;/field&gt;
 *      &lt;/object&gt;
 *      ...
 *  &lt;/objects&gt;
 *  </pre>
 * </p>
 *
 * <p>
 * "Name Format" differs from "Storage ID Format" in that object types and fields are specified by name instead of storage ID.
 * However, it does not support schemas that have duplicate object or field names, or that use names that are not valid XML tags.
 * <pre>
 *  &lt;objects&gt;
 *      &lt;Person id="c8a971e1aef01cc8" version="1"&gt;
 *          &lt;name&gt;George Washington&lt;/name&gt;
 *          &lt;wasPresident&gt;true&lt;/wasPresident&gt;
 *          &lt;attributes&gt;
 *              &lt;entry&gt;
 *                  &lt;key&gt;teeth&lt;/key&gt;
 *                  &lt;value&gt;wooden&lt;/value&gt;
 *              &lt;/entry&gt;
 *          &lt;/attributes&gt;
 *          &lt;spouse&gt;c8b84a08e5c2b1a2&lt;/spouse&gt;
 *      &lt;/Person&gt;
 *      ...
 *  &lt;/objects&gt;
 *  </pre>
 * </p>
 *
 * <p>
 * Some details on the input logic:
 * <ul>
 *  <li>Simple fields that are equal to their default values, and complex fields that are empty, may be omitted on input</li>
 *  <li>The {@code "version"} attribute may be omitted, in which case the default schema version associated with
 *      the {@link Transaction} being written to is assumed.</li>
 *  <li>The {@code "id"} attribute may be omitted, in which case a random unassigned ID is generated</li>
 *  <li>Any object ID (including the {@code "id"} attribute) may have the special form
 *      <code>generated:<i>TYPE</i>:<i>SUFFIX</i></code>, where <code><i>TYPE</i></code> is the object type
 *      storage ID and <code><i>SUFFIX</i></code> is an arbitrary string.
 *      In this case, a random, unassigned object ID for type <code><i>TYPE</i></code> is generated
 *      on first occurrence, and on subsequent occurences the previously generated ID is recalled. This facilitates
 *      input generated via XSL and the {@code generate-id()} function.
 *      The {@linkplain #getGeneratedIdCache configured} {@link GeneratedIdCache} keeps track of generated IDs.</li>
 * </ul>
 * </p>
 */
public class XMLObjectSerializer extends AbstractXMLStreaming {

    public static final QName ELEMENT_TAG = new QName("element");
    public static final QName ENTRY_TAG = new QName("entry");
    public static final QName FIELD_TAG = new QName("field");
    public static final QName KEY_TAG = new QName("key");
    public static final QName OBJECTS_TAG = new QName("objects");
    public static final QName OBJECT_TAG = new QName("object");
    public static final QName VALUE_TAG = new QName("value");

    public static final QName ID_ATTR = new QName("id");
    public static final QName STORAGE_ID_ATTR = new QName("storageId");
    public static final QName VERSION_ATTR = new QName("version");

    private static final Pattern GENERATED_ID_PATTERN = Pattern.compile("generated:([^:]+):(.*)");

    private final Transaction tx;

    private GeneratedIdCache generatedIdCache = new GeneratedIdCache();

    /**
     * Constructor.
     *
     * @param tx {@link Transaction} on which to operate
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public XMLObjectSerializer(Transaction tx) {
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        this.tx = tx;
    }

    /**
     * Get the {@link GeneratedIdCache} associated with this instance.
     */
    public GeneratedIdCache getGeneratedIdCache() {
        return this.generatedIdCache;
    }

    /**
     * Set the {@link GeneratedIdCache} associated with this instance.
     *
     * @throws IllegalArgumentException if {@code generatedIdCache} is null
     */
    public void setGeneratedIdCache(GeneratedIdCache generatedIdCache) {
        if (generatedIdCache == null)
            throw new IllegalArgumentException("null generatedIdCache");
        this.generatedIdCache = generatedIdCache;
    }

    /**
     * Import objects pairs into the {@link Transaction} associated with this instance from the given XML input.
     *
     * <p>
     * The input format is auto-detected for each {@code <object>} based on the presense of the {@code "type"} attribute.
     * </p>
     *
     * @param input XML input
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code input} is null
     */
    public void read(InputStream input) throws XMLStreamException {
        if (input == null)
            throw new IllegalArgumentException("null input");
        this.read(XMLInputFactory.newFactory().createXMLStreamReader(input));
    }

    /**
     * Import objects into the {@link Transaction} associated with this instance from the given XML input.
     * This method expects to see an opening {@code <objects>} as the next event (not counting whitespace, comments, etc.),
     * which is then consumed up through the closing {@code </objects>} event. Therefore this tag could be part of a
     * larger XML document.
     *
     * <p>
     * The input format is auto-detected for each {@code <object>} based on the presense of the {@code "type"} attribute.
     * </p>
     *
     * @param reader XML reader
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code reader} is null
     */
    @SuppressWarnings("unchecked")
    public void read(XMLStreamReader reader) throws XMLStreamException {
        if (reader == null)
            throw new IllegalArgumentException("null reader");
        this.expect(reader, false, OBJECTS_TAG);

        // Build name index for each schema version
        final HashMap<Integer, NameIndex> nameIndexMap = new HashMap<>();
        for (SchemaVersion schemaVersion : this.tx.getSchema().getSchemaVersions().values())
            nameIndexMap.put(schemaVersion.getVersionNumber(), new NameIndex(schemaVersion.getSchemaModel()));

        // Iterate over objects
        QName name;
        while ((name = this.next(reader)) != null) {

            // Determine schema version
            final String versionAttr = this.getAttr(reader, VERSION_ATTR, false);
            final SchemaVersion schemaVersion;
            if (versionAttr != null) {
                try {
                    schemaVersion = this.tx.getSchema().getVersion(Integer.parseInt(versionAttr));
                } catch (IllegalArgumentException e) {
                    throw new XMLStreamException("invalid object schema version `" + versionAttr + "': " + e,
                      reader.getLocation(), e);
                }
            } else
                schemaVersion = this.tx.getSchemaVersion();
            final NameIndex nameIndex = nameIndexMap.get(schemaVersion.getVersionNumber());
            final SchemaModel schemaModel = schemaVersion.getSchemaModel();

            // Determine object type
            String storageIdAttr = this.getAttr(reader, STORAGE_ID_ATTR, false);
            final ObjType objType;
            if (storageIdAttr != null) {
                try {
                    objType = schemaVersion.getSchemaItem(Integer.parseInt(storageIdAttr), ObjType.class);
                } catch (IllegalArgumentException e) {
                    throw new XMLStreamException("invalid object type storage ID `" + storageIdAttr + "': " + e,
                      reader.getLocation(), e);
                }
                if (!name.equals(OBJECT_TAG)) {
                    if (!XMLConstants.NULL_NS_URI.equals(name.getNamespaceURI())) {
                        throw new XMLStreamException("unexpected element <" + name.getPrefix() + ":"
                          + name.getLocalPart() + ">; expected <" + OBJECT_TAG.getLocalPart() + ">", reader.getLocation());
                    }
                    if (!objType.getName().equals(name.getLocalPart())) {
                        throw new XMLStreamException("element <" + name.getLocalPart()
                          + "> does not match storage ID " + objType.getStorageId()
                          + "; should be <" + objType.getName() + ">", reader.getLocation());
                    }
                }
            } else {
                if (!XMLConstants.NULL_NS_URI.equals(name.getNamespaceURI())) {
                    throw new XMLStreamException("unexpected element <" + name.getPrefix() + ":"
                      + name.getLocalPart() + ">; expected object type name", reader.getLocation());
                }
                final SchemaObject schemaObject;
                try {
                    schemaObject = nameIndex.getSchemaObject(name.getLocalPart());
                } catch (IllegalArgumentException e) {
                    throw new XMLStreamException("unexpected element <" + name.getLocalPart()
                      + ">; object type name `" + name.getLocalPart() + "' is invalid for schema version "
                      + schemaVersion.getVersionNumber() + ": " + e, reader.getLocation(), e);
                }
                objType = schemaVersion.getSchemaItem(schemaObject.getStorageId(), ObjType.class);
            }
            final SchemaObject schemaObject = schemaModel.getSchemaObjects().get(objType.getStorageId());

            // Determine object ID
            final String idAttr = this.getAttr(reader, ID_ATTR, true);
            ObjId id;
            if (idAttr == null)
                id = this.tx.create(objType.getStorageId(), schemaVersion.getVersionNumber());
            else {
                try {
                    id = new ObjId(idAttr);
                } catch (IllegalArgumentException e) {
                    final Matcher matcher = GENERATED_ID_PATTERN.matcher(idAttr);
                    if (!matcher.matches())
                        throw new XMLStreamException("invalid object ID `" + idAttr + "'", reader.getLocation());
                    final String typeAttr = matcher.group(1);
                    final String suffix = matcher.group(2);
                    if (!typeAttr.equals("" + objType.getStorageId())) {
                        throw new XMLStreamException("invalid storage ID `" + typeAttr + "' in generated object ID `" + idAttr
                          + "': storage ID does not match storage ID `" + objType.getStorageId() + "' of type `"
                          + objType.getName() + "' in schema version " + schemaVersion.getVersionNumber(), reader.getLocation());
                    }
                    id = this.generatedIdCache.getGeneratedId(this.tx, objType.getStorageId(), suffix);
                }

                // Delete existing object, if any
                this.tx.delete(id);

                // Create new object
                this.tx.create(id, schemaVersion.getVersionNumber());
            }

            // Iterate over fields
            while ((name = this.next(reader)) != null) {
                final QName fieldTagName = name;

                // Determine the field
                storageIdAttr = this.getAttr(reader, STORAGE_ID_ATTR, false);
                final Field<?> field;
                if (storageIdAttr != null) {
                    try {
                        field = objType.getFields().get(Integer.parseInt(storageIdAttr));
                    } catch (IllegalArgumentException e) {
                        throw new XMLStreamException("invalid field storage ID `" + storageIdAttr
                          + "': " + e, reader.getLocation(), e);
                    }
                    if (field == null) {
                        throw new XMLStreamException("unknown field storage ID `" + storageIdAttr + "' for object type `"
                          + objType.getName() + "' #" + objType.getStorageId() + " in schema version "
                          + schemaVersion.getVersionNumber(), reader.getLocation());
                    }
                } else {
                    if (!XMLConstants.NULL_NS_URI.equals(name.getNamespaceURI())) {
                        throw new XMLStreamException("unexpected element <" + name.getPrefix() + ":"
                          + name.getLocalPart() + ">; expected field name", reader.getLocation());
                    }
                    final SchemaField schemaField;
                    try {
                        schemaField = nameIndex.getSchemaField(schemaObject, name.getLocalPart());
                    } catch (IllegalArgumentException e) {
                        throw new XMLStreamException("unexpected element <" + name.getLocalPart() + ">; unknown field `"
                          + name.getLocalPart() + "' in object type `" + objType.getName() + "' #"
                          + objType.getStorageId() + " in schema version " + schemaVersion.getVersionNumber(),
                          reader.getLocation());
                    }
                    if (schemaField == null) {
                        throw new XMLStreamException("unknown field `" + name.getLocalPart() + "' in object type `"
                          + objType.getName() + "' #" + objType.getStorageId() + " in schema version "
                          + schemaVersion.getVersionNumber(), reader.getLocation());
                    }
                    field = objType.getFields().get(schemaField.getStorageId());
                    assert field != null : "field=" + schemaField + " fields=" + objType.getFields();
                }

                // Parse the field
                if (field instanceof SimpleField)
                    this.tx.writeSimpleField(id, field.getStorageId(), this.readSimpleField(reader, (SimpleField<?>)field), false);
                else if (field instanceof CounterField) {
                    final long value;
                    try {
                        value = Long.parseLong(reader.getElementText());
                    } catch (Exception e) {
                        throw new XMLStreamException("invalid counter value for field `" + field.getName() + "': " + e,
                          reader.getLocation(), e);
                    }
                    this.tx.writeCounterField(id, field.getStorageId(), value, false);
                } else if (field instanceof CollectionField) {
                    final SimpleField<?> elementField = ((CollectionField<?, ?>)field).getElementField();
                    final Collection<?> collection;
                    if (field instanceof SetField)
                        collection = this.tx.readSetField(id, field.getStorageId(), false);
                    else if (field instanceof ListField)
                        collection = this.tx.readListField(id, field.getStorageId(), false);
                    else
                        throw new RuntimeException("internal error: " + field);
                    while ((name = this.next(reader)) != null) {
                        if (!ELEMENT_TAG.equals(name)) {
                            throw new XMLStreamException("invalid field element; expected <" + ELEMENT_TAG.getLocalPart()
                              + "> but found opening <" + name.getLocalPart() + ">", reader.getLocation());
                        }
                        ((Collection<Object>)collection).add(this.readSimpleField(reader, elementField));
                    }
                } else if (field instanceof MapField) {
                    final SimpleField<?> keyField = ((MapField<?, ?>)field).getKeyField();
                    final SimpleField<?> valueField = ((MapField<?, ?>)field).getValueField();
                    final Map<?, ?> map = this.tx.readMapField(id, field.getStorageId(), false);
                    while ((name = this.next(reader)) != null) {
                        if (!ENTRY_TAG.equals(name)) {
                            throw new XMLStreamException("invalid map field entry; expected <" + ENTRY_TAG.getLocalPart()
                              + "> but found opening <" + name.getLocalPart() + ">", reader.getLocation());
                        }
                        if (!KEY_TAG.equals(this.next(reader))) {
                            throw new XMLStreamException("invalid map entry key; expected <" + KEY_TAG.getLocalPart()
                              + ">", reader.getLocation());
                        }
                        final Object key = this.readSimpleField(reader, keyField);
                        if (!VALUE_TAG.equals(this.next(reader))) {
                            throw new XMLStreamException("invalid map entry value; expected <" + VALUE_TAG.getLocalPart()
                              + ">", reader.getLocation());
                        }
                        final Object value = this.readSimpleField(reader, valueField);
                        ((Map<Object, Object>)map).put(key, value);
                        if ((name = this.next(reader)) != null) {
                            throw new XMLStreamException("invalid map field entry; expected closing <" + ENTRY_TAG.getLocalPart()
                              + "> but found opening <" + name.getLocalPart() + ">", reader.getLocation());
                        }
                    }
                } else
                    throw new RuntimeException("internal error: " + field);
            }
        }
    }

    /**
     * Export all objects from the {@link Transaction} associated with this instance to the given output.
     *
     * @param output XML output; will not be closed by this method
     * @param nameFormat true for Name Format, false for Storage ID Format
     * @param indent true to indent output, false for all on one line
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code output} is null
     */
    public void write(OutputStream output, boolean nameFormat, boolean indent) throws XMLStreamException {
        if (output == null)
            throw new IllegalArgumentException("null output");
        XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8");
        if (indent)
            xmlWriter = new IndentXMLStreamWriter(xmlWriter);
        xmlWriter.writeStartDocument("UTF-8", "1.0");
        this.write(xmlWriter, nameFormat);
    }

    /**
     * Export all objects from the {@link Transaction} associated with this instance to the given writer.
     *
     * @param writer XML output; will not be closed by this method
     * @param nameFormat true for Name Format, false for Storage ID Format
     * @param indent true to indent output, false for all on one line
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code writer} is null
     */
    public void write(Writer writer, boolean nameFormat, boolean indent) throws XMLStreamException {
        if (writer == null)
            throw new IllegalArgumentException("null writer");
        XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(writer);
        if (indent)
            xmlWriter = new IndentXMLStreamWriter(xmlWriter);
        xmlWriter.writeStartDocument("1.0");
        this.write(xmlWriter, nameFormat);
    }

    private void write(XMLStreamWriter writer, boolean nameFormat) throws XMLStreamException {

        // Gather all known object storage IDs
        final TreeSet<Integer> storageIds = new TreeSet<>();
        for (SchemaVersion schemaVersion : this.tx.getSchema().getSchemaVersions().values())
            storageIds.addAll(schemaVersion.getSchemaModel().getSchemaObjects().keySet());

        // Get corresponding iterators
        final ArrayList<Iterator<ObjId>> iterators = new ArrayList<>(storageIds.size());
        for (int storageId : storageIds)
            iterators.add(this.tx.getAll(storageId).iterator());

        // Output all objects
        this.write(writer, nameFormat, Iterators.concat(iterators.iterator()));
    }

    /**
     * Export the given objects from the {@link Transaction} associated with this instance to the given XML output.
     *
     * <p>
     * This method writes a start element as its first action, allowing the output to be embedded into a larger XML document.
     * Callers not embedding the output may with to precede invocation of this method with a call to
     * {@link XMLStreamWriter#writeStartDocument writer.writeStartDocument()}.
     * </p>
     *
     * @param writer XML writer; will not be closed by this method
     * @param nameFormat true for Name Format, false for Storage ID Format
     * @param iterator iterator of object IDs
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code writer} is null
     */
    public void write(XMLStreamWriter writer, boolean nameFormat, Iterator<ObjId> iterator) throws XMLStreamException {
        if (writer == null)
            throw new IllegalArgumentException("null writer");
        if (iterator == null)
            throw new IllegalArgumentException("null iterator");
        writer.setDefaultNamespace(OBJECTS_TAG.getNamespaceURI());
        writer.writeStartElement(OBJECTS_TAG.getNamespaceURI(), OBJECTS_TAG.getLocalPart());
        while (iterator.hasNext()) {
            final ObjId id = iterator.next();

            // Get object info
            final int typeStorageId = id.getStorageId();
            final int version = this.tx.getSchemaVersion(id);
            final SchemaVersion schemaVersion = this.tx.getSchema().getVersion(version);
            final ObjType objType = schemaVersion.getSchemaItem(typeStorageId, ObjType.class);

            // Get format info
            final QName objectElement = nameFormat ? new QName(objType.getName()) : OBJECT_TAG;
            final int storageIdAttr = nameFormat ? -1 : typeStorageId;

            // Output fields; if all are default, output empty tag
            boolean tagOutput = false;
            for (Field<?> field : objType.getFields().values()) {

                // Determine if field equals its default value; if so, skip it
                if (field.hasDefaultValue(this.tx, id))
                    continue;

                // Output <object> opening tag if not output yet
                if (!tagOutput) {
                    this.writeOpenTag(writer, false, objectElement, storageIdAttr, id, version);
                    tagOutput = true;
                }

                // Output field opening tag
                final QName fieldElement = nameFormat ? new QName(field.getName()) : FIELD_TAG;
                writer.writeStartElement(fieldElement.getNamespaceURI(), fieldElement.getLocalPart());
                if (!nameFormat) {
                    writer.writeAttribute(STORAGE_ID_ATTR.getNamespaceURI(),
                      STORAGE_ID_ATTR.getLocalPart(), "" + field.getStorageId());
                }

                // Output field value
                if (field instanceof SimpleField)
                    this.writeSimpleField(writer, (SimpleField<?>)field, this.tx.readSimpleField(id, field.getStorageId(), false));
                else if (field instanceof CounterField)
                    writer.writeCharacters("" + this.tx.readCounterField(id, field.getStorageId(), false));
                else if (field instanceof SetField) {
                    final SimpleField<?> elementField = ((SetField<?>)field).getElementField();
                    for (Object element : this.tx.readSetField(id, field.getStorageId(), false)) {
                        writer.writeStartElement(ELEMENT_TAG.getNamespaceURI(), ELEMENT_TAG.getLocalPart());
                        this.writeSimpleField(writer, elementField, element);
                        writer.writeEndElement();
                    }
                } else if (field instanceof ListField) {
                    final SimpleField<?> elementField = ((ListField<?>)field).getElementField();
                    for (Object element : this.tx.readListField(id, field.getStorageId(), false)) {
                        writer.writeStartElement(ELEMENT_TAG.getNamespaceURI(), ELEMENT_TAG.getLocalPart());
                        this.writeSimpleField(writer, elementField, element);
                        writer.writeEndElement();
                    }
                } else if (field instanceof MapField) {
                    final SimpleField<?> keyField = ((MapField<?, ?>)field).getKeyField();
                    final SimpleField<?> valueField = ((MapField<?, ?>)field).getValueField();
                    for (Map.Entry<?, ?> entry : this.tx.readMapField(id, field.getStorageId(), false).entrySet()) {
                        writer.writeStartElement(ENTRY_TAG.getNamespaceURI(), ENTRY_TAG.getLocalPart());
                        writer.writeStartElement(KEY_TAG.getNamespaceURI(), KEY_TAG.getLocalPart());
                        this.writeSimpleField(writer, keyField, entry.getKey());
                        writer.writeEndElement();
                        writer.writeStartElement(VALUE_TAG.getNamespaceURI(), VALUE_TAG.getLocalPart());
                        this.writeSimpleField(writer, valueField, entry.getValue());
                        writer.writeEndElement();
                        writer.writeEndElement();
                    }
                } else
                    throw new RuntimeException("internal error: " + field);

                // Output field closing tag
                writer.writeEndElement();
            }

            // Output empty opening tag if not output yet, otherwise closing tag
            if (!tagOutput)
                this.writeOpenTag(writer, true, objectElement, storageIdAttr, id, version);
            else
                writer.writeEndElement();
        }
        writer.writeEndElement();
        writer.flush();
    }

// Internal methods

    private <T> boolean isDefaultValue(SimpleField<T> field, Object value) throws XMLStreamException {
        final ByteWriter writer = new ByteWriter();
        final FieldType<T> fieldType = field.getFieldType();
        fieldType.write(writer, fieldType.validate(value));
        return Arrays.equals(writer.getBytes(), field.getFieldType().getDefaultValue());
    }

    private <T> void writeSimpleField(XMLStreamWriter writer, SimpleField<T> field, Object value) throws XMLStreamException {
        final FieldType<T> fieldType = field.getFieldType();
        writer.writeCharacters(fieldType.toString(fieldType.validate(value)));
    }

    private <T> T readSimpleField(XMLStreamReader reader, SimpleField<T> field) throws XMLStreamException {
        final FieldType<T> fieldType = field.getFieldType();

        // Get text
        final String text;
        try {
            text = reader.getElementText();
        } catch (Exception e) {
            throw new XMLStreamException("invalid value for field `" + field.getName() + "': " + e, reader.getLocation(), e);
        }

        // Handle generated ID's for reference fields
        if (field instanceof ReferenceField) {
            final Matcher matcher = GENERATED_ID_PATTERN.matcher(text);
            if (matcher.matches()) {
                final String typeAttr = matcher.group(1);
                final int storageId;
                try {
                    storageId = Integer.parseInt(typeAttr);
                } catch (IllegalArgumentException e) {
                    throw new XMLStreamException("invalid storage ID `" + typeAttr + "' in generated object ID `"
                      + text + "': " + e, reader.getLocation(), e);
                }
                try {
                    field.getVersion().getSchemaItem(storageId, ObjType.class);
                } catch (IllegalArgumentException e) {
                    throw new XMLStreamException("invalid object type storage ID `" + typeAttr + "' in generated object ID `"
                      + text + "': " + e, reader.getLocation(), e);
                }
                final String suffix = matcher.group(2);
                return fieldType.validate(this.generatedIdCache.getGeneratedId(this.tx, storageId, suffix));
            }
        }

        // Parse field value
        try {
            return fieldType.fromString(new ParseContext(text));
        } catch (Exception e) {
            throw new XMLStreamException("invalid value `" + text + "' for field `" + field.getName() + "': " + e,
              reader.getLocation(), e);
        }
    }

    private void writeOpenTag(XMLStreamWriter writer, boolean empty, QName element, int storageId, ObjId id, int version)
      throws XMLStreamException {
        if (empty)
            writer.writeEmptyElement(element.getNamespaceURI(), element.getLocalPart());
        else
            writer.writeStartElement(element.getNamespaceURI(), element.getLocalPart());
        if (storageId != -1)
            writer.writeAttribute(STORAGE_ID_ATTR.getNamespaceURI(), STORAGE_ID_ATTR.getLocalPart(), "" + storageId);
        writer.writeAttribute(ID_ATTR.getNamespaceURI(), ID_ATTR.getLocalPart(), id.toString());
        writer.writeAttribute(VERSION_ATTR.getNamespaceURI(), VERSION_ATTR.getLocalPart(), "" + version);
    }

    private QName next(XMLStreamReader reader) throws XMLStreamException {
        while (true) {
            if (!reader.hasNext())
                throw new XMLStreamException("unexpected end of input", reader.getLocation());
            final int eventType = reader.next();
            if (eventType == XMLStreamConstants.END_ELEMENT)
                return null;
            if (eventType == XMLStreamConstants.START_ELEMENT)
                return reader.getName();
        }
    }
}

