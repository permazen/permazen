
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Reference paths.
 *
 * <p>
 * A Reference path consists of:
 * <ul>
 *  <li>A {@linkplain #getStartType starting Java object type},</li>
 *  <li>A final {@linkplain #getTargetType target object type} and {@linkplain #getTargetField target field}
 *      within that type, and</li>
 *  <li>An intermediate chain of zero or more {@linkplain #getReferenceFields reference fields} through which an
 *      instance of the target type can be reached from an instance of the starting type.</li>
 * </ul>
 * </p>
 *
 * <p>
 * Reference paths can be described by the combination of (a) the starting Java object type, and (b) the path of reference
 * fields and final target field in a {@link String} form consisting of field names separated by period ({@code "."}) characters.
 * All of the fields except the last must be reference fields.
 * </p>
 *
 * <p>
 * For example, path {@code "parent.age"} starting from object type {@code Person} might refer to the age of
 * the parent of a {@code Person}.
 * </p>
 *
 * <p>
 * When a complex field appears in a reference path, both the name of the complex field and the specific sub-field
 * being traversed should appear, e.g., {@code "mymap.key.somefield"}. For set and list fields, the (only) sub-field is
 * named {@code "element"}, while for map fields the sub-fields are named {@code "key"} and {@code "value"}.
 * </p>
 *
 * <p>
 * <b>Fields of Sub-Types</b>
 * </p>
 *
 * <p>
 * In some cases, a field may not exist in a Java object type, but it does exist in a some sub-type of that type. For example:
 * </p>
 *
 * <p>
 * <pre>
 * &#64;JSimpleClass(storageId = 10)
 * public class Person {
 *
 *     &#64;JSimpleSetField(storageId = 11)
 *     public abstract Set&lt;Person&gt; <b>getFriends</b>();
 *
 *     &#64;OnChange("friends.element.<b>name</b>")
 *     private void friendNameChanged(SimpleFieldChange&lt;NamedPerson, String&gt; change) {
 *         // ... do whatever
 *     }
 * }
 *
 * &#64;JSimpleClass(storageId = 20)
 * public class NamedPerson extends Person {
 *
 *     &#64;JSimpleField(storageId = 21)
 *     public abstract String <b>getName</b>();
 *     public abstract void setName(String name);
 * }
 * </pre>
 * Here the path {@code "friends.element.name"} is technically incorrect because {@code "friends.element"}
 * has type {@code Person}, and {@code "name"} is a field of {@code NamedPerson}, not {@code Person}. However, this will
 * still work as long as there is no ambiguity, i.e., in this example, there are no other sub-types of {@code Person}
 * with a field named {@code "name"}. Note also in the example above the
 * {@link org.jsimpledb.change.SimpleFieldChange} parameter to the method
 * {@code friendNameChanged()} necessarily has generic type {@code NamedPerson}, not {@code Person}.
 * </p>
 */
public class ReferencePath {

    final TypeToken<?> startType;
    final TypeToken<?> targetType;
    final JField targetField;
    final JComplexField targetSuperField;
    final ArrayList<JReferenceField> referenceFields = new ArrayList<>();

    /**
     * Constructor.
     *
     * @param jlayer {@link JLayer} with which to resolve object and field names
     * @param startType starting Java type for the path
     * @param path dot-separated path of zero or more reference fields, followed by a target field
     * @param lastIsSubField true if the last field can be a complex sub-field but not a complex field, false for the reverse,
     *  or null for don't care
     * @throws IllegalArgumentException if {@code jlayer}, {@code startType}, or {@code path} is null
     * @throws IllegalArgumentException if {@code path} is invalid
     */
    ReferencePath(JLayer jlayer, TypeToken<?> startType, String path, Boolean lastIsSubField) {

        // Sanity check
        if (jlayer == null)
            throw new IllegalArgumentException("null jlayer");
        if (startType == null)
            throw new IllegalArgumentException("null startType");
        if (path == null)
            throw new IllegalArgumentException("null path");
        final String errorPrefix = "invalid path `" + path + "': ";
        if (path.length() == 0)
            throw new IllegalArgumentException(errorPrefix + "path is empty");
        this.startType = startType;

        // Split the path into field names
        final ArrayDeque<String> fieldNames = new ArrayDeque<>();
        while (true) {
            final int dot = path.indexOf('.');
            if (dot == -1) {
                if (path.length() == 0)
                    throw new IllegalArgumentException(errorPrefix + "ends in `.'");
                fieldNames.add(path);
                break;
            }
            if (dot == 0)
                throw new IllegalArgumentException(errorPrefix + "contains an empty path component");
            fieldNames.add(path.substring(0, dot));
            path = path.substring(dot + 1);
        }

        // Initialize loop state
        TypeToken<?> currentType = this.startType;
        JField currentField = null;
        JComplexField currentSuperField = null;

        // Parse field names
        final ArrayList<Integer> storageIds = new ArrayList<>();
        while (!fieldNames.isEmpty()) {
            final String fieldName = fieldNames.removeFirst();
            String description = "field `" + fieldName + "' in type " + currentType;

            // Find all JFields matching 'fieldName' in some JClass whose type matches 'typeToken'
            final HashMap<JClass<?>, JField> matchingFields = new HashMap<>();
            for (JClass<?> jclass : jlayer.jclasses.values()) {
                if (!currentType.isAssignableFrom(jclass.typeToken))
                    continue;
                final JField jfield = jclass.jfieldsByName.get(fieldName);
                if (jfield == null)
                    continue;
                matchingFields.put(jclass, jfield);
            }

            // None found?
            if (matchingFields.isEmpty()) {
                throw new IllegalArgumentException(errorPrefix + "there is no field named `"
                  + fieldName + "' in (any sub-type of) " + currentType);
            }

            // Get the field; if multiple fields are found, verify they all are really the same field
            currentField = matchingFields.values().iterator().next();
            for (JField otherField : matchingFields.values()) {
                if (otherField.storageId != currentField.storageId) {
                    throw new IllegalArgumentException(errorPrefix + "there are multiple different fields named `"
                      + fieldName + "' in sub-types of type " + currentType);
                }
            }
            currentSuperField = null;

            // Get the lowest common super-type among the matching JClass's
            for (JClass<?> jclass : matchingFields.keySet()) {
                boolean commonSupertype = true;
                for (JClass<?> other : matchingFields.keySet()) {
                    if (!jclass.typeToken.isAssignableFrom(other.typeToken)) {
                        commonSupertype = false;
                        break;
                    }
                }
                if (commonSupertype) {
                    currentType = jclass.typeToken;
                    break;
                }
            }

            // Handle complex fields
            if (currentField instanceof JComplexField) {
                final JComplexField complexField = (JComplexField)currentField;

                // Last field?
                if (fieldNames.isEmpty()) {
                    if (lastIsSubField != null && lastIsSubField) {
                        throw new IllegalArgumentException(errorPrefix + "path may not end on complex " + description
                          + "; a sub-field of must be specified");
                    }
                    break;
                }
                currentSuperField = complexField;

                // Get sub-field
                final String subFieldName = fieldNames.removeFirst();
                description = "sub-field `" + subFieldName + "' of complex " + description;
                try {
                    currentField = complexField.getSubField(subFieldName);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(errorPrefix + "invalid " + description + ": " + e.getMessage(), e);
                }
            }

            // Last field?
            if (fieldNames.isEmpty()) {
                if (currentField.parent instanceof JComplexField && lastIsSubField != null && !lastIsSubField) {
                    throw new IllegalArgumentException(errorPrefix + "path may not end on " + description
                      + "; specify the complex field itself instead");
                }
                break;
            }

            // Field is not last, so it must be a reference field
            if (!(currentField instanceof JReferenceField))
                throw new IllegalArgumentException(errorPrefix + description + " is not a reference field");
            final JReferenceField referenceField = (JReferenceField)currentField;

            // Add reference field to the reference field list
            this.referenceFields.add(referenceField);

            // Advance through the reference
            currentType = referenceField.typeToken;
        }

        // Done
        this.targetType = currentType;
        this.targetField = currentField;
        this.targetSuperField = currentSuperField;
    }

    /**
     * Get the Java type at which this path starts.
     *
     * <p>
     * If there are zero {@linkplain #getReferenceFields reference fields} in this path, then this will
     * equal the {@linkplain #getTargetType target object type}, or possibly a super-type if the target
     * field exists only in a sub-type.
     * </p>
     */
    public TypeToken<?> getStartType() {
        return this.startType;
    }

    /**
     * Get the Java type at which this path ends.
     *
     * <p>
     * If there are zero {@linkplain #getReferenceFields reference fields} in this path, then this will
     * equal the Java type of the {@linkplain #getStartType starting object type}, or possibly a sub-type
     * if the target field exists only in a sub-type.
     * </p>
     */
    public TypeToken<?> getTargetType() {
        return this.targetType;
    }

    /**
     * Get the storage ID associated with the target field in the {@linkplain #getTargetType target object type}.
     *
     * <p>
     * This is just the storage ID of the last field in the path.
     * </p>
     */
    public int getTargetField() {
        return this.targetField.storageId;
    }

    /**
     * Get the storage ID associated with the complex field containing the {@linkplain #getTargetField target field}
     * a sub-field, in the case that the target field is a sub-field of a complex field.
     *
     * @return target field's complex super-field storage ID, or zero if the target field is not a complex sub-field
     */
    public int getTargetSuperField() {
        return this.targetSuperField != null ? this.targetSuperField.storageId : 0;
    }

    /**
     * Get the storage IDs of the reference fields in this path.
     *
     * <p>
     * The path may be empty, i.e., zero references are traversed in the path.
     * </p>
     *
     * <p>
     * Otherwise, the first field is a field in the {@linkplain #getStartType starting object type} and
     * the last field is field in some object type that refers to the {@linkplain #getTargetType target object type}.
     * </p>
     *
     * @return zero or more reference field storage IDs
     */
    public int[] getReferenceFields() {
        final int[] storageIds = new int[this.referenceFields.size()];
        for (int i = 0; i < storageIds.length; i++)
            storageIds[i] = this.referenceFields.get(i).storageId;
        return storageIds;
    }

    /**
     * Get the {@link String} form of the path associated with this instance.
     */
    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        for (JReferenceField referenceField : this.referenceFields)
            this.appendField(buf, referenceField);
        this.appendField(buf, this.targetField);
        return buf.toString();
    }

    private void appendField(StringBuilder buf, JField field) {
        if (buf.length() > 0)
            buf.append('.');
        if (field instanceof JSimpleField) {
            final JSimpleField simpleField = (JSimpleField)field;
            if (simpleField.parent instanceof JComplexField) {
                final JComplexField parent = (JComplexField)simpleField.parent;
                buf.append(parent.name).append('.').append(parent.getSubFieldName(simpleField));
                return;
            }
        }
        buf.append(field.name);
    }
}

