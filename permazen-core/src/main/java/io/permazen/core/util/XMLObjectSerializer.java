
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
import io.permazen.util.XMLUtil;

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
import java.util.function.Function;
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
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/prism.min.js"></script>
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/components/prism-xml-doc.min.js"></script>
 * <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/themes/prism.min.css" rel="stylesheet"/>
 *
 * <p>
 * <b>XML Structure</b>
 *
 * <p>
 * The overall XML format looks like this:
 *
 * <pre><code class="language-xml">
 *  &lt;database&gt;
 *      &lt;schemas&gt;
 *          ...
 *      &lt;/schemas&gt;
 *      &lt;objects&gt;
 *          ...
 *      &lt;/objects&gt;
 *  &lt;/database&gt;
 * </code></pre>
 *
 * The {@code <schemas>} tag contains the database's schema definitions. Providing these definitions makes it
 * possible to import into a completely empty {@link Database}, i.e., one with no prior knowledge of the objects' schema(s).
 *
 * <p>
 * The {@code <objects>} tag contains the actual object data. Each object may belong to a different schema.
 *
 * <p>
 * <b>Object XML Format</b>
 *
 * <p>
 * There are two supported XML formats for {@code <object>} tags, <b>plain</b> and <b>custom</b>.
 *
 * <p>
 * The plain object format uses standardized XML element names and identifies object types and fields with a {@code "name"}
 * attribute. Because Permazen requires object type and field names to contain only characters that are valid in XML attributes,
 * this format supports all possible database object and field names. Example:
 *
 * <pre><code class="language-xml">
 *  &lt;object type="Person" id="64a971e1aef01cc8"&gt;
 *      &lt;field name="name"&gt;George Washington&lt;/field&gt;
 *      &lt;field name="wasPresident"&gt;true&lt;/field&gt;
 *      &lt;field name="attributes"&gt;
 *          &lt;entry&gt;
 *              &lt;key&gt;teeth&lt;/key&gt;
 *              &lt;value&gt;wooden&lt;/value&gt;
 *          &lt;/entry&gt;
 *      &lt;/field&gt;
 *      &lt;field name="spouse"&gt;c8b84a08e5c2b1a2&lt;/field&gt;
 *  &lt;/object&gt;
 * </code></pre>
 *
 * <p>
 * The custom object format uses the object type and field names as XML element names, and is therefore more readable.
 * However, it doesn't support object type and field names that are not valid XML element names. Equivalent example:
 *
 * <pre><code class="language-xml">
 *  &lt;Person id="64a971e1aef01cc8"&gt;
 *      &lt;name&gt;George Washington&lt;/name&gt;
 *      &lt;wasPresident&gt;true&lt;/wasPresident&gt;
 *      &lt;attributes&gt;
 *          &lt;entry&gt;
 *              &lt;key&gt;teeth&lt;/key&gt;
 *              &lt;value&gt;wooden&lt;/value&gt;
 *          &lt;/entry&gt;
 *      &lt;/attributes&gt;
 *      &lt;spouse&gt;c8b84a08e5c2b1a2&lt;/spouse&gt;
 *  &lt;/Person&gt;
 * </code></pre>
 *
 * When parsing input, the format is auto-detected on a per-XML element basis, depending on whether or not there is a
 * {@code type="..."} attribute (for objects) or a {@code name="..."} attribute (for fields). When generating an output
 * element in the custom format, the plain format is used for any element which would otherwise result in invalid XML.
 * You can also configure all elements to be generated in the plain format for uniformity.
 *
 * <p>
 * <b>Schema Determination</b>
 *
 * <p>
 * On input, the schema against which each {@code <object>} element is interpreted is determined as follows:
 * <ul>
 *  <li>If the {@code <object>} tag has a {@code schema} attribute, use the previously defined {@link SchemaModel}
 *      having the specified {@link SchemaId}.
 *  <li>Otherwise, if the containing {@code <objects>} tag has a {@code schema} attribute, use that {@link SchemaId}
 *  <li>Otherwise no explicit schema is specified, so use the schema associated with the {@link Transaction} being written into.
 * </ul>
 *
 * In all cases, the selected schema must either be defined in the XML in the {@code <schemas>} section,
 * or already exist in the target {@link Transaction}.
 *
 * <p>
 * <b>Object ID Generation</b>
 *
 * <p>
 * Any object ID (including the {@code "id"} attribute) may have the special form
 * <code>generated:<i>TYPE</i>:<i>SUFFIX</i></code>, where <code><i>TYPE</i></code> is the object type name
 * and <code><i>SUFFIX</i></code> is an arbitrary string. In this case, a random, unassigned object ID
 * is generated on the first occurrence, and on subsequent occurences the previously generated ID is recalled.
 * This facilitates automatically generated input (e.g., using XSL's {@code generate-id()} function),
 * including forward references.
 *
 * <p>
 * When using object ID generation, the {@linkplain #getGeneratedIdCache configured} {@link GeneratedIdCache}
 * keeps track of generated IDs.</li>
 *
 * <p>
 * <b>Storage ID's and Portability</b>
 *
 * <p>
 * Permazen assigns storage ID's to schema elements (object types, fields, indexes, etc.) dynamically when a schema
 * is first registered in the database. When exporting schemas and objects from an existing database, you have a choice
 * whether to include or exclude these storage ID assignments in the export.
 *
 * <p>
 * If you include them, then the XML file is able to exactly reproduce the keys and values in the underlying key/value
 * database. In particular, each object will be assigned the same object ID it had when it was originally exported,
 * and on import if an object with a given object ID already exists, it will be replaced.
 * However, the XML file will be incompatible with (i.e., fail to import into) any database that has different storage
 * ID assignments for any of the object types or fields being imported.
 *
 * <p>
 * You may instead omit storage ID assignments from the export. This means the data can be imported freely into any database,
 * but it will no longer create the original object ID's, keys and values. In this scenario, object ID's are exported in the
 * form <code>generated:<i>TYPE</i>:<i>SUFFIX</i></code>, and so a new object is created as each {@code <object>} is imported.
 *
 * <p>
 * In general you should include storage ID's if you are exporting data that will later be imported back into the same database
 * (an "edit" operation) or an empty database (a "copy" operation), and exclude storage ID's if you are exporting data that will
 * later be imported into some other, existing database (a "merge" operation).
 *
 * <p>
 * <b>Other Details</b>
 *
 * <ul>
 *  <li>The {@code "id"} attribute may be omitted from an {@code <object>} tag; in this case, a random, unassigned ID
 *      is generated. In this case, the object will not be referenced by any other object.
 *  <li>Simple fields that are equal to their default values and complex fields that are empty may be omitted</li>
 *  <li>XML element and annotation names are in the null XML namespace; elements and annotations in other namespaces
 *      are ignored</li>
 *  <li>It is allowed for a custom XML tag to have a redundant {@code name} attribute, as long as the name matches</li>
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

    private static final String GENERATED_PREFIX = "generated";
    private static final Pattern GENERATED_ID_PATTERN = Pattern.compile(Pattern.quote(GENERATED_PREFIX) + ":([^:]+):(.*)");

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
                throw this.newInvalidInputException(reader, e, "invalid object type \"%s\": %s", typeName, e.getMessage());
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
                String fieldName = this.getAttr(reader, NAME_ATTR, false);
                if (fieldName == null) {
                    if (!XMLConstants.NULL_NS_URI.equals(tagName.getNamespaceURI())) {
                        throw this.newInvalidInputException(reader,
                          "unexpected element <%s:%s>; expected <%s> or field name",
                          tagName.getPrefix(), tagName.getLocalPart(), FIELD_TAG);
                    }
                    fieldName = tagName.getLocalPart();
                } else if (!tagName.equals(FIELD_TAG)) {
                    if (!XMLConstants.NULL_NS_URI.equals(tagName.getNamespaceURI())) {
                        throw this.newInvalidInputException(reader,
                          "unexpected element <%s:%s>; expected <%s>",
                          tagName.getPrefix(), tagName.getLocalPart(), FIELD_TAG);
                    }
                    if (!fieldName.equals(tagName.getLocalPart())) {
                        throw this.newInvalidInputException(reader,
                          "element <%s> does not match field name \"%s\" (should be <%s> or <%s>)",
                          tagName.getLocalPart(), typeName, fieldName, FIELD_TAG);
                    }
                }

                // Get the corresponding field
                final Field<?> field;
                if ((field = objType.getFields().get(fieldName)) == null) {
                    throw this.newInvalidInputException(reader,
                      "unknown field \"%s\" in object type \"%s\" in schema %s",
                      fieldName, objType.getName(), schema.getSchemaId());
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
     * Export all objects from the {@link Transaction} associated with this instance to the given output
     * using the default configuration.
     *
     * <p>
     * Equivalent to: <code>write(output, OutputOptions.builder().build())</code>.
     *
     * @param output XML output; will not be closed by this method
     * @return the number of objects written
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code output} is null
     */
    public long write(OutputStream output) throws XMLStreamException {
        return this.write(output, OutputOptions.builder().build());
    }

    /**
     * Export all objects from the {@link Transaction} associated with this instance to the given writer
     * using the default configuration.
     *
     * <p>
     * Equivalent to: <code>write(writer, OutputOptions.OutputOptions.builder().build())</code>.
     *
     * @param writer XML output; will not be closed by this method
     * @return the number of objects written
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if {@code writer} is null
     */
    public long write(Writer writer) throws XMLStreamException {
        return this.write(writer, OutputOptions.builder().build());
    }

    /**
     * Export all objects from the {@link Transaction} associated with this instance to the given output.
     *
     * @param output XML output; will not be closed by this method
     * @param options output options
     * @return the number of objects written
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if either parameter is null
     */
    public long write(OutputStream output, OutputOptions options) throws XMLStreamException {
        Preconditions.checkArgument(output != null, "null output");
        Preconditions.checkArgument(options != null, "null options");
        XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8");
        if (options.isPrettyPrint())
            writer = new IndentXMLStreamWriter(writer);
        writer.writeStartDocument("UTF-8", "1.0");
        return this.write(writer, options);
    }

    /**
     * Export all objects from the {@link Transaction} associated with this instance to the given writer.
     *
     * @param writer XML output; will not be closed by this method
     * @param options output options
     * @return the number of objects written
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if either parameter is null
     */
    public long write(Writer writer, OutputOptions options) throws XMLStreamException {
        Preconditions.checkArgument(writer != null, "null writer");
        Preconditions.checkArgument(options != null, "null options");
        XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(writer);
        if (options.isPrettyPrint())
            xmlWriter = new IndentXMLStreamWriter(xmlWriter);
        xmlWriter.writeStartDocument("1.0");
        return this.write(xmlWriter, options);
    }

    private long write(XMLStreamWriter writer, OutputOptions options) throws XMLStreamException {

        // Open <database>
        writer.setDefaultNamespace(DATABASE_TAG.getNamespaceURI());
        writer.writeStartElement(DATABASE_TAG.getNamespaceURI(), DATABASE_TAG.getLocalPart());

        // Write <schemas>
        writer.writeStartElement(SCHEMAS_TAG.getNamespaceURI(), SCHEMAS_TAG.getLocalPart());
        try (Stream<? extends SchemaModel> schemaModels = options.schemaGenerator().apply(this.tx)) {
            for (Iterator<? extends SchemaModel> i = schemaModels.iterator(); i.hasNext(); )
                i.next().writeXML(writer, options.isIncludeStorageIds(), options.isPrettyPrint());
        }
        writer.writeEndElement();       // </schemas>

        // Write <objects>
        final long count;
        try (Stream<? extends ObjId> objIds = options.objectGenerator().apply(this.tx)) {
            count = this.writeObjects(writer, options, objIds);
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
     * @param options output options
     * @param objIds object IDs
     * @return the number of objects written
     * @throws XMLStreamException if an error occurs
     * @throws IllegalArgumentException if any parameter is null
     */
    public long writeObjects(XMLStreamWriter writer, OutputOptions options, Stream<? extends ObjId> objIds)
      throws XMLStreamException {

        // Sanity check
        Preconditions.checkArgument(writer != null, "null writer");
        Preconditions.checkArgument(options != null, "null options");
        Preconditions.checkArgument(objIds != null, "null objIds");

        // Create opening tag
        writer.setDefaultNamespace(OBJECTS_TAG.getNamespaceURI());
        writer.writeStartElement(OBJECTS_TAG.getNamespaceURI(), OBJECTS_TAG.getLocalPart());

        // Set default schema ID (ony if default schema is non-empty)
        final SchemaId defaultSchemaId = Optional.of(this.tx.getSchema().getSchemaModel())
          .filter(schemaModel -> !schemaModel.isEmpty())
          .map(SchemaModel::getSchemaId)
          .orElse(null);
        if (defaultSchemaId != null)
            this.writeAttribute(writer, SCHEMA_ATTR, defaultSchemaId);

        // Write objects
        long count = 0;
        for (Iterator<? extends ObjId> i = objIds.iterator(); i.hasNext(); ) {
            final ObjId id = i.next();

            // Get object info
            final ObjType objType = this.tx.getObjType(id);
            final Schema schema = objType.getSchema();
            final SchemaId schemaId = schema.getSchemaId();

            // Should we use plain or custom format for the object element?
            final boolean customObjectElement = options.isElementsAsNames() && XMLUtil.isValidName(objType.getName());

            // Get format info
            final QName objectElement = customObjectElement ? new QName(objType.getName()) : OBJECT_TAG;
            final String typeNameAttr = customObjectElement ? null : objType.getName();
            final SchemaId schemaAttr = !schemaId.equals(defaultSchemaId) ? schemaId : null;

            // Output fields; if all are default, output empty tag
            boolean tagOutput = false;
            ArrayList<Field<?>> fieldList = new ArrayList<>(objType.getFields().values());
            if (customObjectElement)
                Collections.sort(fieldList, Comparator.comparing(Field::getName));
            for (Field<?> field : fieldList) {

                // Determine if field equals its default value; if so, skip it
                if (this.omitDefaultValueFields && field.hasDefaultValue(this.tx, id))
                    continue;

                // Output <object> opening tag if not output yet
                if (!tagOutput) {
                    this.writeOpenTag(writer, options, objType, false, objectElement, typeNameAttr, id, schemaAttr);
                    tagOutput = true;
                }

                // Should we use plain or custom format for the field element?
                final boolean customFieldElement = options.isElementsAsNames() && XMLUtil.isValidName(field.getName());

                // Get tag name
                final QName fieldTag = customFieldElement ? new QName(field.getName()) : FIELD_TAG;

                // Special case for simple fields, which use empty tags when null
                if (field instanceof SimpleField) {
                    final Object value = this.tx.readSimpleField(id, field.getName(), false);
                    if (value == null || this.fieldTruncationLength == 0)
                        writer.writeEmptyElement(fieldTag.getNamespaceURI(), fieldTag.getLocalPart());
                    else
                        writer.writeStartElement(fieldTag.getNamespaceURI(), fieldTag.getLocalPart());
                    if (!customFieldElement)
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
                if (!customFieldElement)
                    this.writeAttribute(writer, NAME_ATTR, field.getName());

                // Output field value
                if (field instanceof CounterField)
                    this.writeCharacters(writer, "" + this.tx.readCounterField(id, field.getName(), false));
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
                this.writeOpenTag(writer, options, objType, true, objectElement, typeNameAttr, id, schemaAttr);
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
            this.writeCharacters(writer, text);
            return;
        }
        this.writeCharacters(writer, text.substring(0, this.fieldTruncationLength));
        this.writeCharacters(writer, "...[truncated]");
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
            final T value = encoding.fromString(text);
            if (value == null)
                throw new RuntimeException("internal error: Encoding.fromString() returned null value");
            return value;
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

    private void writeOpenTag(XMLStreamWriter writer, OutputOptions options, ObjType objType,
      boolean empty, QName element, String typeName, ObjId id, SchemaId schemaId) throws XMLStreamException {
        if (empty)
            writer.writeEmptyElement(element.getNamespaceURI(), element.getLocalPart());
        else
            writer.writeStartElement(element.getNamespaceURI(), element.getLocalPart());
        if (typeName != null)
            this.writeAttribute(writer, TYPE_ATTR, typeName);
        String idText = id.toString();
        if (!options.isIncludeStorageIds())
            idText = String.format("%s:%s:%s", GENERATED_PREFIX, objType.getName(), idText);
        this.writeAttribute(writer, ID_ATTR, idText);
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

// OutputOptions

    /**
     * Options for the output XML format produced by {@link XMLObjectSerializer}.
     */
    public static final class OutputOptions {

        private final Function<? super Transaction, ? extends Stream<? extends SchemaModel>> schemaGenerator;
        private final Function<? super Transaction, ? extends Stream<? extends ObjId>> objectGenerator;
        private final boolean elementsAsNames;
        private final boolean includeStorageIds;
        private final boolean prettyPrint;

        private OutputOptions(Builder builder) {
            this.schemaGenerator = builder.schemaGenerator;
            this.objectGenerator = builder.objectGenerator;
            this.elementsAsNames = builder.elementsAsNames;
            this.includeStorageIds = builder.includeStorageIds;
            this.prettyPrint = builder.prettyPrint;
        }

        /**
         * Get the function that streams the {@link SchemaModel}s to include in the output.
         *
         * @return {@link SchemaModel} streaming function, never null
         */
        public Function<? super Transaction, ? extends Stream<? extends SchemaModel>> schemaGenerator() {
            return this.schemaGenerator != null ? this.schemaGenerator :
              tx -> tx.getSchemaBundle().getSchemasBySchemaIndex().values().stream().map(schema -> schema.getSchemaModel(true));
        }

        /**
         * Get the function that streams the objects to include in the output (identified by {@link ObjId}).
         *
         * @return {@link ObjId} streaming function, never null
         */
        public Function<? super Transaction, ? extends Stream<? extends ObjId>> objectGenerator() {
            return this.objectGenerator != null ? this.objectGenerator : tx -> tx.getAll().stream();
        }

        /**
         * Determine whether to output XML element names that are based on the corresponding object or field name.
         *
         * @return whether to use custom XML element names
         */
        public boolean isElementsAsNames() {
            return this.elementsAsNames;
        }

        /**
         * Determine whether to include storage ID's in the output.
         *
         * @return whether to include storage ID's
         */
        public boolean isIncludeStorageIds() {
            return this.includeStorageIds;
        }

        /**
         * Determine whether to pretty-print the XML, i.e., indent and include storage ID comments.
         *
         * @return whether to pretty-print
         */
        public boolean isPrettyPrint() {
            return this.prettyPrint;
        }

    // Other Methods

        public static Builder builder() {
            return new Builder();
        }

        /**
         * Create a {@link Builder} that is pre-configured as a copy of this instance.
         *
         * @return new pre-configured builder
         */
        public Builder copy() {
            return new Builder()
              .schemaGenerator(this.schemaGenerator)
              .objectGenerator(this.objectGenerator)
              .elementsAsNames(this.elementsAsNames)
              .includeStorageIds(this.includeStorageIds)
              .prettyPrint(this.prettyPrint);
        }

    // Builder

        /**
         * Builder for {@link OutputOptions}.
         */
        public static final class Builder implements Cloneable {

            private Function<? super Transaction, ? extends Stream<? extends SchemaModel>> schemaGenerator;
            private Function<? super Transaction, ? extends Stream<? extends ObjId>> objectGenerator;
            private boolean elementsAsNames = true;
            private boolean includeStorageIds;
            private boolean prettyPrint = true;

            private Builder() {
            }

            /**
             * Configure a custom function that streams the {@link SchemaModel}s to include in the output.
             *
             * <p>
             * The generated {@link SchemaModel}s should always include storage ID's, so that the {@link #includeStorageIds}
             * option can decide whether they are expressed in the output.
             *
             * @param schemaGenerator custom {@link SchemaModel} streaming function, or null to stream all schemas in the database
             */
            public Builder schemaGenerator(Function<? super Transaction, ? extends Stream<? extends SchemaModel>> schemaGenerator) {
                this.schemaGenerator = schemaGenerator;
                return this;
            }

            /**
             * Configure a custom function that streams the objects to include in the output (identified by {@link ObjId}).
             *
             * @return custom {@link ObjId} streaming function, or null to stream all objects in the database
             */
            public Builder objectGenerator(Function<? super Transaction, ? extends Stream<? extends ObjId>> objectGenerator) {
                this.objectGenerator = objectGenerator;
                return this;
            }

            /**
             * Configure whether to output XML element names that are based on the corresponding object or field name.
             *
             * <p>
             * The default value is true.
             *
             * @param elementsAsNames whether to use custom XML element names
             * @return this instance
             */
            public Builder elementsAsNames(boolean elementsAsNames) {
                this.elementsAsNames = elementsAsNames;
                return this;
            }

            /**
             * Configure whether to include storage ID's in the output.
             *
             * <p>
             * The default value is false.
             *
             * @param includeStorageIds whether to include storage ID's
             * @return this instance
             */
            public Builder includeStorageIds(boolean includeStorageIds) {
                this.includeStorageIds = includeStorageIds;
                return this;
            }

            /**
             * Configure whether to pretty-print the XML, i.e., indent and include storage ID comments.
             *
             * <p>
             * The default value is true.
             *
             * @param prettyPrint whether to pretty-print
             * @return this instance
             */
            public Builder prettyPrint(boolean prettyPrint) {
                this.prettyPrint = prettyPrint;
                return this;
            }

            /**
             * Create a new {@link OutputOptions} from this instance.
             *
             * @return new output config
             */
            public OutputOptions build() {
                return new OutputOptions(this);
            }

            /**
             * Clone this instance.
             *
             * @return clone of this instance
             */
            public Builder clone() {
                try {
                    return (Builder)super.clone();
                } catch (CloneNotSupportedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
