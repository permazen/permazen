
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import com.google.common.base.Preconditions;

import io.permazen.core.CollectionField;
import io.permazen.core.CounterField;
import io.permazen.core.Database;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.DetachedTransaction;
import io.permazen.core.Field;
import io.permazen.core.InvalidSchemaException;
import io.permazen.core.ListField;
import io.permazen.core.MapField;
import io.permazen.core.ObjId;
import io.permazen.core.ObjType;
import io.permazen.core.ReferenceField;
import io.permazen.core.Schema;
import io.permazen.core.SetField;
import io.permazen.core.SimpleField;
import io.permazen.core.Transaction;
import io.permazen.core.UnknownTypeException;
import io.permazen.encoding.Encoding;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;
import io.permazen.schema.SchemaObjectType;
import io.permazen.util.AbstractXMLStreaming;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.xml.IndentXMLStreamWriter;

/**
 * Utility methods for serializing and deserializing {@link Database} objects in a {@link Transaction} to/from XML.
 *
 * <p>
 * <b>XML Structure</b>
 *
 * <p>
 * The overall XML format looks like this:
 *  <pre>
 *  &lt;database&gt;
 *      &lt;schemas&gt;
 *          ...
 *      &lt;/schemas&gt;
 *      &lt;objects&gt;
 *          ...
 *      &lt;/objects&gt;
 *  &lt;/database&gt;
 *  </pre>
 *
 * The {@code <schemas>} tag contains the database's schema definitions. Providing these definitions makes it
 * possible to import into a completely empty {@link Database}, i.e., one with no prior knowledge of the objects' schema(s).
 *
 * <p>
 * The {@code <objects>} tag contains the actual object data. Each object may belong to a different schema.
 *
 * <p>
 * <b>Object Formats</b>
 *
 * <p>
 * There are two supported formats for {@code <object>} tags. The "plain" object format uses standard XML elements
 * and identifies objects and fields with a {@code "name"} attribute using standardized. This format supports all
 * possible database object and field names:
 *  <pre>
 *  &lt;objects schema="Schema_12e983a72e72ed56741ddc45e47d3377"&gt;
 *      &lt;object type="Person" id="64a971e1aef01cc8"&gt;
 *          &lt;field name="name"&gt;George Washington&lt;/field&gt;
 *          &lt;field name="wasPresident"&gt;true&lt;/field&gt;
 *          &lt;field name="attributes"&gt;
 *              &lt;entry&gt;
 *                  &lt;key&gt;teeth&lt;/key&gt;
 *                  &lt;value&gt;wooden&lt;/value&gt;
 *              &lt;/entry&gt;
 *          &lt;/field&gt;
 *          &lt;field name="spouse"&gt;c8b84a08e5c2b1a2&lt;/field&gt;
 *      &lt;/object&gt;
 *      ...
 *  &lt;/objects&gt;
 *  </pre>
 *
 * The "custom" object format uses custom XML tags to specify object types and field names and is more readable.
 * However, it doesn't support object type names and field names that are not valid XML tags:
 * <pre>
 *  &lt;objects&gt;
 *      &lt;Person id="64a971e1aef01cc8"&gt;
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
 *
 * When parsing input, the format is auto-detected on a per-element basis, depending on whether or not there is a
 * {@code type="..."} attribute (for objects) or {@code name="..."} attribute (for fields).
 *
 * <p>
 * <b>Schema Determination</b>
 *
 * <p>
 * The schema against which each {@code <object>} is interpreted is determined as follows:
 * <ul>
 *  <li>If the {@code <object>} tag has a {@code schema="..."} attribute, interpret it as the {@link SchemaId}
 *      of a {@link SchemaModel} and use the corresponding schema.
 *  <li>Otherwise, if the containing {@code <objects>} tag has a {@code schema="..."} attribute, use that.
 *  <li>Either way, an explicit schema must either be defined earlier in the XML or already exist in the database.
 *  <li>Otherwise if no explicit schema is specified, use the schema associated with the {@link Transaction} being written into.
 * </ul>
 *
 * <p>
 * <b>Object ID Generation</b>
 *
 * <p>
 * Any object ID (including the {@code "id"} attribute) may have the special form
 * <code>generated:<i>TYPE</i>:<i>SUFFIX</i></code>, where <code><i>TYPE</i></code> is the object type name
 * and <code><i>SUFFIX</i></code> is an arbitrary string. In this case, a random, unassigned object ID
 * is generated on the first occurrence, and on subsequent occurences the previously generated ID is recalled.
 * This facilitates automatically generated input (e.g., using XSL's {@code generate-id()} function)
 * and forward references.
 *
 * <p>
 * When using object ID generation, the {@linkplain #getGeneratedIdCache configured} {@link GeneratedIdCache}
 * keeps track of generated IDs.</li>
 *
 * <p>
 * <b>Other Details</b>
 * <ul>
 *  <li>The {@code "id"} attribute may be omitted, in which case a random unassigned ID is generated</li>
 *  <li>Simple fields that are equal to their default values and complex fields that are empty may be omitted</li>
 *  <li>XML elements and annotations are expected to be in the null namespace; elements and annotations in other
 *      namespaces are ignored</li>
 * </ul>
 */
public class XMLObjectSerializer extends AbstractXMLStreaming {

    /**
     * The supported XML namespace URI.
     *
     * <p>
     * Currently this is {@link XMLConstants#NULL_NS_URI}, i.e., the null/default namespace.
     *
     * <p>
     * XML tags and attributes whose names are in other namespaces are ignored.
     */
    public static final String NS_URI = XMLConstants.NULL_NS_URI;

    public static final QName ELEMENT_TAG = new QName("element");
    public static final QName ENTRY_TAG = new QName("entry");
    public static final QName FIELD_TAG = new QName("field");
    public static final QName KEY_TAG = new QName("key");
    public static final QName OBJECTS_TAG = new QName("objects");
    public static final QName OBJECT_TAG = new QName("object");
    public static final QName DATABASE_TAG = new QName("database");
    public static final QName SCHEMAS_TAG = new QName("schemas");
    public static final QName VALUE_TAG = new QName("value");

    public static final QName ID_ATTR = new QName("id");
    public static final QName NAME_ATTR = new QName("name");
    public static final QName NULL_ATTR = new QName("null");
    public static final QName SCHEMA_ATTR = new QName("schema");
    public static final QName TYPE_ATTR = new QName("type");

    private static final Pattern GENERATED_ID_PATTERN = Pattern.compile("generated:([^:]+):(.*)");

    private final Transaction tx;

    private GeneratedIdCache generatedIdCache = new GeneratedIdCache();
    private final ObjIdMap<ReferenceField> unresolvedReferences = new ObjIdMap<>();
    private boolean omitDefaultValueFields = true;
    private int fieldTruncationLength = -1;

// Constructors

    /**
     * Constructor.
     *
     * @param tx {@link Transaction} on which to operate
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public XMLObjectSerializer(Transaction tx) {
        Preconditions.checkArgument(tx != null, "null tx");
        this.tx = tx;
    }

// Configuration Methods

    /**
     * Get the maximum length (number of characters) of any written simple field.
     *
     * <p>
     * By default, this value is set to {@code -1}, i.e., truncation is disabled.
     *
     * @return maximum simple field length, or zero for empty simple fields, or {@code -1} if truncation is disabled
     * @see #setFieldTruncationLength setFieldTruncationLength()
     */
    public int getFieldTruncationLength() {
        return this.fieldTruncationLength;
    }

    /**
     * Set the maximum length (number of characters) of any written simple field.
     *
     * <p>
     * Simple field values longer than this will be truncated. If set to zero, all simple field values are written as empty tags.
     * If set to {@code -1}, truncation is disabled.
     *
     * <p>
     * Truncation is mainly useful for generating human-readable output without very long lines.
     * Obviously, when truncation is enabled, the resulting output, although still valid XML, will
     * be missing some information and therefore cannot successfully be read back in by this class.
     *
     * @param length maximum simple field length, or zero for empty simple fields, or {@code -1} to disable truncation
     * @throws IllegalArgumentException if {@code length < -1}
     */
    public void setFieldTruncationLength(int length) {
        Preconditions.checkArgument(length >= -1, "length < -1");
        this.fieldTruncationLength = length;
    }

    /**
     * Get whether to omit fields whose value equals the default value for the field's type.
     *
     * <p>
     * Default true.
     *
     * @return whether to omit fields with default values
     */
    public boolean isOmitDefaultValueFields() {
        return this.omitDefaultValueFields;
    }

    /**
     * Set whether to omit fields whose value equals the default value for the field's type.
     *
     * <p>
     * Default true.
     *
     * @param omitDefaultValueFields true to omit fields with default values
     */
    public void setOmitDefaultValueFields(final boolean omitDefaultValueFields) {
        this.omitDefaultValueFields = omitDefaultValueFields;
    }

    /**
     * Get all unresolved forward object references.
     *
     * <p>
     * When {@link #read(InputStream, boolean) read()} is invoked with {@code allowUnresolvedReferences = true},
     * unresolved forward object references do not trigger an exception; this allows forward references to span
     * multiple invocations. Instead, these references are collected and made available to the caller in the returned map.
     * Callers may also modify the returned map as desired between invocations.
     *
     * @return mapping from unresolved forward object reference to some referring field
     */
    public ObjIdMap<ReferenceField> getUnresolvedReferences() {
        return this.unresolvedReferences;
    }

    /**
     * Get the {@link GeneratedIdCache} associated with this instance.
     *
     * @return the associated {@link GeneratedIdCache}
     */
    public GeneratedIdCache getGeneratedIdCache() {
        return this.generatedIdCache;
    }

    /**
     * Set the {@link GeneratedIdCache} associated with this instance.
     *
     * @param generatedIdCache the {@link GeneratedIdCache} for this instance to use
     * @throws IllegalArgumentException if {@code generatedIdCache} is null
     */
    public void setGeneratedIdCache(GeneratedIdCache generatedIdCache) {
        Preconditions.checkArgument(generatedIdCache != null, "null generatedIdCache");
        this.generatedIdCache = generatedIdCache;
    }

// I/O Methods

    /**
     * Import objects pairs into the {@link Transaction} associated with this instance from the given XML input.
     *
     * <p>
     * This is a convenience method, equivalent to:
     * <blockquote><pre>
     * read(input, false)
     * </pre></blockquote>
     *
     * @param input XML input
     * @return the number of objects read
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code input} is null
     */
    public int read(InputStream input) throws XMLStreamException {
        return this.read(input, false);
    }

    /**
     * Import objects pairs into the {@link Transaction} associated with this instance from the given XML input.
     *
     * <p>
     * The input format is auto-detected for each {@code <object>} based on the presence of the {@code "type"} attribute.
     *
     * <p>
     * Can optionally check for unresolved object references after reading is complete. If this checking is enabled,
     * an exception is thrown if any unresolved references remain. In any case, the unresolved references are available
     * via {@link #getUnresolvedReferences}.
     *
     * @param input XML input
     * @param allowUnresolvedReferences true to allow unresolved references, false to throw an exception
     * @return the number of objects read
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code input} is null
     * @throws DeletedObjectException if {@code allowUnresolvedReferences} is true and any unresolved references
     *  remain when loading is complete
     */
    public int read(InputStream input, boolean allowUnresolvedReferences) throws XMLStreamException {
        Preconditions.checkArgument(input != null, "null input");
        final int count = this.read(XMLInputFactory.newFactory().createXMLStreamReader(input));
        if (!allowUnresolvedReferences && !this.unresolvedReferences.isEmpty()) {
            throw new DeletedObjectException(this.unresolvedReferences.keySet().iterator().next(),
              this.unresolvedReferences.size() + " unresolved reference(s) remain");
        }
        return count;
    }

    /**
     * Import objects into the {@link Transaction} associated with this instance from the given XML input.
     * This method expects to see an opening {@code <objects>} as the next event (not counting whitespace, comments, etc.),
     * which is then consumed up through the closing {@code </objects>} event. Therefore this tag could be part of a
     * larger XML document.
     *
     * <p>
     * The input format is auto-detected for each {@code <object>} based on the presence of the {@code "type"} attribute.
     *
     * @param reader XML reader
     * @return the number of objects read
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code reader} is null
     */
    @SuppressWarnings("unchecked")
    public int read(XMLStreamReader reader) throws XMLStreamException {

        // Sanity check
        Preconditions.checkArgument(reader != null, "null reader");

        // Parse opening <database> tag and get default schema
        this.expect(reader, false, DATABASE_TAG);

        // Parse opening <schemas> tag and get default schema
        this.expect(reader, false, SCHEMAS_TAG);

        // Read schemas
        final ArrayList<SchemaModel> schemaList = new ArrayList<>();
        while (this.expect(reader, true, io.permazen.schema.XMLConstants.SCHEMA_MODEL_TAG)) {

            // Parse schema
            final SchemaModel schemaModel = new SchemaModel();
            try {
                schemaModel.readXML(reader, true);
                schemaModel.lockDown(true);
                schemaModel.validate();
            } catch (IllegalArgumentException | XMLStreamException | InvalidSchemaException e) {
                throw this.newInvalidInputException(reader, e, "invalid schema: %s", e.getMessage());
            }

            // Add to map
            schemaList.add(schemaModel);
        }

        // Add schemas to transaction
        for (SchemaModel schemaModel : schemaList)
            this.tx.addSchema(schemaModel);

        // Parse opening <objects> tag and get default schema
        this.expect(reader, false, OBJECTS_TAG);
        final SchemaId defaultSchemaId = this.getSchemaIdAttr(reader, SCHEMA_ATTR);

        // Create a detached transaction in which we will construct each object we're importing
        final DetachedTransaction tempTx = this.tx.createDetachedTransaction();

        // Iterate over objects
        int count;
        QName tagName;
        for (count = 0; (tagName = this.next(reader)) != null; count++) {

            // Determine schema
            final SchemaId schemaId = Optional.ofNullable(this.getSchemaIdAttr(reader, SCHEMA_ATTR)).orElse(defaultSchemaId);
            final Schema schema;
            if (schemaId != null) {
                try {
                    schema = this.tx.getSchemaBundle().getSchema(schemaId);
                } catch (IllegalArgumentException e) {
                    throw this.newInvalidInputException(reader, e, "invalid schema \"%s\": %s", schemaId, e.getMessage());
                }
            } else
                schema = this.tx.getSchema();
            final SchemaModel schemaModel = schema.getSchemaModel();

            // Determine object element format and get object's type name
            String typeName = this.getAttr(reader, TYPE_ATTR, false);
            if (typeName == null) {                                                     // custom mode
                if (!XMLConstants.NULL_NS_URI.equals(tagName.getNamespaceURI())) {
                    throw this.newInvalidInputException(reader,
                      "unexpected element <%s:%s>; expected <%s> or object type name",
                      tagName.getPrefix(), tagName.getLocalPart(), OBJECT_TAG);
                }
                typeName = tagName.getLocalPart();
            } else if (!tagName.equals(OBJECT_TAG)) {                                   // plain mode
                if (!XMLConstants.NULL_NS_URI.equals(tagName.getNamespaceURI())) {
                    throw this.newInvalidInputException(reader,
                      "unexpected element <%s:%s>; expected <%s>",
                      tagName.getPrefix(), tagName.getLocalPart(), OBJECT_TAG);
                }
                if (!typeName.equals(tagName.getLocalPart())) {
                    throw this.newInvalidInputException(reader,
                      "element <%s> does not match object type name \"%s\" (should be <%s> or <%s>)",
                      tagName.getLocalPart(), typeName, typeName, OBJECT_TAG);
                }
            }

            // Get the corresponding object type
            final ObjType objType;
            try {
                objType = schema.getObjType(typeName);
            } catch (UnknownTypeException e) {
                final String message = typeName.equals(OBJECT_TAG.toString()) ?
                  String.format("<%s> tag is missing \"%s\" attribute", OBJECT_TAG, TYPE_ATTR) :
                  String.format("invalid object type \"%s\": %s", typeName, e.getMessage());
                throw this.newInvalidInputException(reader, e, "%s", message);
            }

            // Reset detached transaction to discard previous object
            tempTx.reset();

            // Determine object ID and create object in detached transaction
            ObjId id;
            final String idAttr = this.getAttr(reader, ID_ATTR, false);
            if (idAttr == null)
                id = tempTx.create(typeName, schema.getSchemaId());                 // create a random new object
            else {

                // Parse id
                try {
                    id = new ObjId(idAttr);
                } catch (IllegalArgumentException e) {

                    // Check for generated:TYPE:SUFFIX
                    if ((id = this.parseGeneratedId(reader, schema, idAttr, objType)) == null)
                        throw this.newInvalidInputException(reader, e, "invalid object ID \"%s\"", idAttr);
                }

                // Create the specified object
                tempTx.create(id, schema.getSchemaId());
            }

            // Iterate over fields
            final SchemaObjectType schemaObjectType = schemaModel.getSchemaObjectTypes().get(objType.getName());
            while ((tagName = this.next(reader)) != null) {

                // Determine the field
                final String fieldNameAttr = this.getAttr(reader, NAME_ATTR, false);
                final Field<?> field;
                if (fieldNameAttr == null) {
                    if (!XMLConstants.NULL_NS_URI.equals(tagName.getNamespaceURI())) {
                        throw this.newInvalidInputException(reader,
                          "unexpected element <%s:%s>; expected field name",
                          tagName.getPrefix(), tagName.getLocalPart());
                    }
                    if ((field = objType.getFields().get(tagName.getLocalPart())) == null) {
                        throw this.newInvalidInputException(reader,
                          "unexpected element <%s>; unknown field \"%s\" in object type \"%s\" in schema \"%s\"",
                          tagName.getLocalPart(), tagName.getLocalPart(), objType.getName(), schema.getSchemaId());
                    }
                } else if ((field = objType.getFields().get(fieldNameAttr)) == null) {
                    throw this.newInvalidInputException(reader,
                      "unknown field \"%s\" in object type \"%s\" in schema %s",
                      fieldNameAttr, objType.getName(), schema.getSchemaId());
                }

                // Parse the field
                if (field instanceof SimpleField)
                    tempTx.writeSimpleField(id, field.getName(), this.readSimpleField(reader, (SimpleField<?>)field), false);
                else if (field instanceof CounterField) {
                    final long value;
                    try {
                        value = Long.parseLong(reader.getElementText());
                    } catch (Exception e) {
                        throw this.newInvalidInputException(reader, e,
                          "invalid counter value for field \"%s\": %s", field.getName(), e.getMessage());
                    }
                    tempTx.writeCounterField(id, field.getName(), value, false);
                } else if (field instanceof CollectionField) {
                    final SimpleField<?> elementField = ((CollectionField<?, ?>)field).getElementField();
                    final Collection<?> collection;
                    if (field instanceof SetField)
                        collection = tempTx.readSetField(id, field.getName(), false);
                    else if (field instanceof ListField)
                        collection = tempTx.readListField(id, field.getName(), false);
                    else
                        throw new RuntimeException("internal error: " + field);
                    while ((tagName = this.next(reader)) != null) {
                        if (!ELEMENT_TAG.equals(tagName)) {
                            throw this.newInvalidInputException(reader,
                              "invalid field element; expected <%s> but found opening <%s>",
                              ELEMENT_TAG.getLocalPart(), tagName.getLocalPart());
                        }
                        ((Collection<Object>)collection).add(this.readSimpleField(reader, elementField));
                    }
                } else if (field instanceof MapField) {
                    final SimpleField<?> keyField = ((MapField<?, ?>)field).getKeyField();
                    final SimpleField<?> valueField = ((MapField<?, ?>)field).getValueField();
                    final Map<?, ?> map = tempTx.readMapField(id, field.getName(), false);
                    while ((tagName = this.next(reader)) != null) {
                        if (!ENTRY_TAG.equals(tagName)) {
                            throw this.newInvalidInputException(reader,
                              "invalid map field entry; expected <%s> but found opening <%s>",
                              ENTRY_TAG.getLocalPart(), tagName.getLocalPart());
                        }
                        if (!KEY_TAG.equals(this.next(reader))) {
                            throw this.newInvalidInputException(reader,
                              "invalid map entry key; expected <%s>", KEY_TAG.getLocalPart());
                        }
                        final Object key = this.readSimpleField(reader, keyField);
                        if (!VALUE_TAG.equals(this.next(reader))) {
                            throw this.newInvalidInputException(reader,
                              "invalid map entry value; expected <%s>", VALUE_TAG.getLocalPart());
                        }
                        final Object value = this.readSimpleField(reader, valueField);
                        ((Map<Object, Object>)map).put(key, value);
                        if ((tagName = this.next(reader)) != null) {
                            throw this.newInvalidInputException(reader,
                              "invalid map field entry; expected closing <%s> but found opening <%s>",
                              ENTRY_TAG.getLocalPart(), tagName.getLocalPart());
                        }
                    }
                } else
                    throw new RuntimeException("internal error: " + field);
            }

            // Copy over object, replacing any previous
            tempTx.copy(id, this.tx, false, false, this.unresolvedReferences, null);

            // Removed the copied object from deleted assignments, as any forward reference to it is now resolved
            this.unresolvedReferences.remove(id);
        }

        // Done
        return count;
    }

    private SchemaId getSchemaIdAttr(XMLStreamReader reader, QName attrName) throws XMLStreamException {
        final String attr = this.getAttr(reader, attrName, false);
        if (attr == null)
            return null;
        try {
            return new SchemaId(attr);
        } catch (IllegalArgumentException e) {
            throw this.newInvalidInputException(reader, e, "invalid schema ID \"%s\": %s", attr, e.getMessage());
        }
    }

    /**
     * Export all objects from the {@link Transaction} associated with this instance to the given output.
     *
     * @param output XML output; will not be closed by this method
     * @param customFormat true for custom format, false for plain Format
     * @param indent true to indent output, false for all on one line
     * @return the number of objects written
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code output} is null
     */
    public int write(OutputStream output, boolean customFormat, boolean indent) throws XMLStreamException {
        Preconditions.checkArgument(output != null, "null output");
        XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8");
        if (indent)
            xmlWriter = new IndentXMLStreamWriter(xmlWriter);
        xmlWriter.writeStartDocument("UTF-8", "1.0");
        return this.write(xmlWriter, customFormat);
    }

    /**
     * Export all objects from the {@link Transaction} associated with this instance to the given writer.
     *
     * @param writer XML output; will not be closed by this method
     * @param customFormat true for custom format, false for plain Format
     * @param indent true to indent output, false for all on one line
     * @return the number of objects written
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code writer} is null
     */
    public int write(Writer writer, boolean customFormat, boolean indent) throws XMLStreamException {
        Preconditions.checkArgument(writer != null, "null writer");
        XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(writer);
        if (indent)
            xmlWriter = new IndentXMLStreamWriter(xmlWriter);
        xmlWriter.writeStartDocument("1.0");
        return this.write(xmlWriter, customFormat);
    }

    private int write(XMLStreamWriter writer, boolean customFormat) throws XMLStreamException {

        // Open <database>
        writer.setDefaultNamespace(DATABASE_TAG.getNamespaceURI());
        writer.writeStartElement(DATABASE_TAG.getNamespaceURI(), DATABASE_TAG.getLocalPart());

        // Write <schemas>
        writer.writeStartElement(SCHEMAS_TAG.getNamespaceURI(), SCHEMAS_TAG.getLocalPart());
        for (Schema schema : this.tx.getSchemaBundle().getSchemasBySchemaIndex().values())
            schema.getSchemaModel(true).writeXML(writer, true);         // always include storage ID assignments
        writer.writeEndElement();       // </schemas>

        // Write <objects>
        final int count;
        try (Stream<ObjId> objIds = this.tx.getAll().stream()) {
            count = this.write(writer, customFormat, objIds);
        }

        // Done
        writer.writeEndElement();       // </database>
        writer.flush();
        return count;
    }

    /**
     * Export the specified objects from the {@link Transaction} associated with this instance to the given XML output.
     *
     * <p>
     * This method writes a start element as its first action, allowing the output to be embedded into a larger XML document.
     * Callers not embedding the output may with to precede invocation of this method with a call to
     * {@link XMLStreamWriter#writeStartDocument writer.writeStartDocument()}.
     *
     * @param writer XML writer; will not be closed by this method
     * @param customFormat true for custom format, false for plain Format
     * @param objIds object IDs
     * @return the number of objects written
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code writer} or {@code objIds} is null
     */
    public int write(XMLStreamWriter writer, boolean customFormat, Stream<? extends ObjId> objIds) throws XMLStreamException {

        // Sanity check
        Preconditions.checkArgument(writer != null, "null writer");
        Preconditions.checkArgument(objIds != null, "null objIds");

        // Create opening tag
        writer.setDefaultNamespace(OBJECTS_TAG.getNamespaceURI());
        writer.writeStartElement(OBJECTS_TAG.getNamespaceURI(), OBJECTS_TAG.getLocalPart());

        // Write default schema ID
        final SchemaId defaultSchemaId = this.tx.getSchema().getSchemaId();
        this.writeAttribute(writer, SCHEMA_ATTR, defaultSchemaId);

        // Write objects
        int count = 0;
        for (Iterator<? extends ObjId> i = objIds.iterator(); i.hasNext(); ) {
            final ObjId id = i.next();

            // Get object info
            final ObjType objType = this.tx.getObjType(id);
            final Schema schema = objType.getSchema();
            final SchemaId schemaId = schema.getSchemaId();

            // Get format info
            final QName objectElement = customFormat ? new QName(objType.getName()) : OBJECT_TAG;
            final String typeNameAttr = customFormat ? null : objType.getName();
            final SchemaId schemaAttr = !schemaId.equals(defaultSchemaId) ? schemaId : null;

            // Output fields; if all are default, output empty tag
            boolean tagOutput = false;
            ArrayList<Field<?>> fieldList = new ArrayList<>(objType.getFields().values());
            if (customFormat)
                Collections.sort(fieldList, Comparator.comparing(Field::getName));
            for (Field<?> field : fieldList) {

                // Determine if field equals its default value; if so, skip it
                if (this.omitDefaultValueFields && field.hasDefaultValue(this.tx, id))
                    continue;

                // Output <object> opening tag if not output yet
                if (!tagOutput) {
                    this.writeOpenTag(writer, false, objectElement, typeNameAttr, id, schemaAttr);
                    tagOutput = true;
                }

                // Get tag name
                final QName fieldTag = customFormat ? new QName(field.getName()) : FIELD_TAG;

                // Special case for simple fields, which use empty tags when null
                if (field instanceof SimpleField) {
                    final Object value = this.tx.readSimpleField(id, field.getName(), false);
                    if (value == null || this.fieldTruncationLength == 0)
                        writer.writeEmptyElement(fieldTag.getNamespaceURI(), fieldTag.getLocalPart());
                    else
                        writer.writeStartElement(fieldTag.getNamespaceURI(), fieldTag.getLocalPart());
                    if (!customFormat)
                        this.writeAttribute(writer, NAME_ATTR, field.getName());
                    if (value != null && this.fieldTruncationLength != 0) {
                        this.writeSimpleFieldText(writer, (SimpleField<?>)field, value);
                        writer.writeEndElement();
                    } else if (value == null)
                        this.writeAttribute(writer, NULL_ATTR, "true");
                    continue;
                }

                // Output field opening tag
                writer.writeStartElement(fieldTag.getNamespaceURI(), fieldTag.getLocalPart());
                if (!customFormat)
                    this.writeAttribute(writer, NAME_ATTR, field.getName());

                // Output field value
                if (field instanceof CounterField)
                    writer.writeCharacters("" + this.tx.readCounterField(id, field.getName(), false));
                else if (field instanceof CollectionField) {
                    final SimpleField<?> elementField = ((CollectionField<?, ?>)field).getElementField();
                    final Iterable<?> collection = field instanceof SetField ?
                      this.tx.readSetField(id, field.getName(), false) :
                      this.tx.readListField(id, field.getName(), false);
                    for (Object element : collection)
                        this.writeSimpleTag(writer, ELEMENT_TAG, elementField, element);
                } else if (field instanceof MapField) {
                    final SimpleField<?> keyField = ((MapField<?, ?>)field).getKeyField();
                    final SimpleField<?> valueField = ((MapField<?, ?>)field).getValueField();
                    for (Map.Entry<?, ?> entry : this.tx.readMapField(id, field.getName(), false).entrySet()) {
                        writer.writeStartElement(ENTRY_TAG.getNamespaceURI(), ENTRY_TAG.getLocalPart());
                        this.writeSimpleTag(writer, KEY_TAG, keyField, entry.getKey());
                        this.writeSimpleTag(writer, VALUE_TAG, valueField, entry.getValue());
                        writer.writeEndElement();
                    }
                } else
                    throw new RuntimeException("internal error: " + field);

                // Output field closing tag
                writer.writeEndElement();
            }

            // Output empty opening tag if not output yet, otherwise closing tag
            if (!tagOutput)
                this.writeOpenTag(writer, true, objectElement, typeNameAttr, id, schemaAttr);
            else
                writer.writeEndElement();
            count++;
        }

        // Done
        writer.writeEndElement();
        writer.flush();
        return count;
    }

// Internal methods

    private void writeSimpleTag(XMLStreamWriter writer, QName tag, SimpleField<?> field, Object value)
      throws XMLStreamException {
        if (value != null && this.fieldTruncationLength != 0) {
            writer.writeStartElement(tag.getNamespaceURI(), tag.getLocalPart());
            this.writeSimpleFieldText(writer, field, value);
            writer.writeEndElement();
        } else {
            writer.writeEmptyElement(tag.getNamespaceURI(), tag.getLocalPart());
            if (value == null)
                this.writeAttribute(writer, NULL_ATTR, "true");
        }
    }

    private <T> void writeSimpleFieldText(XMLStreamWriter writer, SimpleField<T> field, Object value) throws XMLStreamException {
        final Encoding<T> encoding = field.getEncoding();
        String text = encoding.toString(encoding.validate(value));
        final int length = text.length();
        if (this.fieldTruncationLength == -1 || length <= this.fieldTruncationLength) {
            writer.writeCharacters(text);
            return;
        }
        writer.writeCharacters(text.substring(0, this.fieldTruncationLength));
        writer.writeCharacters("...[truncated]");
    }

    private <T> T readSimpleField(XMLStreamReader reader, SimpleField<T> field) throws XMLStreamException {

        // Get encoding
        final Encoding<T> encoding = field.getEncoding();

        // Check for null
        final String nullAttr = this.getAttr(reader, NULL_ATTR, false);
        boolean isNull = false;
        if (nullAttr != null) {
            switch (nullAttr) {
            case "true":
            case "false":
                isNull = Boolean.valueOf(nullAttr);
                break;
            default:
                throw this.newInvalidInputException(reader,
                  "invalid value \"%s\" for \"%s\" attribute: value must be \"true\" or \"false\"",
                  nullAttr, NULL_ATTR.getLocalPart());
            }
        }

        // Get text content
        final String text;
        try {
            text = reader.getElementText();
        } catch (Exception e) {
            throw this.newInvalidInputException(reader, e,
              "invalid value for field \"%s\": %s", field.getName(), e.getMessage());
        }

        // If null, verify there is no text content
        if (isNull) {
            if (text.length() != 0)
                throw this.newInvalidInputException(reader, "text content not allowed for values with null=\"true\"");
            return null;
        }

        // Handle generated ID's for reference fields
        if (field instanceof ReferenceField) {
            final ObjId id = this.parseGeneratedId(reader, field.getSchema(), text, null);
            if (id != null)
                return encoding.validate(id);
        }

        // Parse field value
        try {
            return encoding.fromString(text);
        } catch (Exception e) {
            throw this.newInvalidInputException(reader, e,
              "invalid value \"%s\" for field \"%s\": %s", text, field.getName(), e.getMessage());
        }
    }

    // Parse a generated object ID
    private ObjId parseGeneratedId(XMLStreamReader reader, Schema schema, String text, ObjType expectedType)
      throws XMLStreamException {

        // Sanity check
        Preconditions.checkArgument(reader != null);
        Preconditions.checkArgument(schema != null);
        Preconditions.checkArgument(text != null);

        // Check for generated:TYPE:SUFFIX
        final Matcher matcher = GENERATED_ID_PATTERN.matcher(text);
        if (!matcher.matches())
            return null;

        // Extract the components
        final String typeName = matcher.group(1);
        final String suffix = matcher.group(2);

        // Get the object type
        final ObjType objType;
        try {
            objType = schema.getObjType(typeName);
        } catch (UnknownTypeException e) {
            throw this.newInvalidInputException(reader, e, "invalid object type \"%s\": %s", typeName, e.getMessage());
        }

        // Verify object type
        if (expectedType != null && objType != expectedType) {
            throw this.newInvalidInputException(reader,
              "invalid object type \"%s\": expected \"%s\"", typeName, expectedType.getName());
        }

        // Get the generated ID
        return this.generatedIdCache.getGeneratedId(this.tx, objType.getName(), suffix);
    }

    private void writeOpenTag(XMLStreamWriter writer, boolean empty, QName element, String typeName, ObjId id, SchemaId schemaId)
      throws XMLStreamException {
        if (empty)
            writer.writeEmptyElement(element.getNamespaceURI(), element.getLocalPart());
        else
            writer.writeStartElement(element.getNamespaceURI(), element.getLocalPart());
        if (typeName != null)
            this.writeAttribute(writer, TYPE_ATTR, typeName);
        this.writeAttribute(writer, ID_ATTR, id);
        if (schemaId != null)
            this.writeAttribute(writer, SCHEMA_ATTR, schemaId);
    }

    private void writeAttribute(XMLStreamWriter writer, QName attr, Object value) throws XMLStreamException {
        final String ns = attr.getNamespaceURI();
        if (ns == null || ns.length() == 0)
            writer.writeAttribute(attr.getLocalPart(), "" + value);
        else
            writer.writeAttribute(attr.getNamespaceURI(), attr.getLocalPart(), "" + value);
    }

    /**
     * Skip forward until either the next opening tag is reached, or the currently open tag is closed.
     * This override ignores XML tags that are not in our namespace.
     */
    @Override
    protected QName next(XMLStreamReader reader) throws XMLStreamException {
        while (true) {
            final QName name = super.next(reader);
            if (name == null || NS_URI.equals(name.getNamespaceURI()))
                return name;
            this.skip(reader);
        }
    }
}
