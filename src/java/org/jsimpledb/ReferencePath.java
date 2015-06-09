
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.jsimpledb.util.CastFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 * <p>
 * In cases where multiple sub-types of a common super-type type have fields with the same name but different storage IDs,
 * the storage ID may be explicitly specified as a suffix, for example, {@code "name#123"}.
 * </p>
 *
 * <p>
 * Reference paths are created via {@link JSimpleDB#parseReferencePath JSimpleDB.parseReferencePath()}.
 * </p>
 *
 * @see JSimpleDB#parseReferencePath JSimpleDB.parseReferencePath()
 */
public class ReferencePath {

    final Class<?> startType;
    final Class<?> targetType;
    final JFieldInfo targetFieldInfo;
    final JComplexFieldInfo targetSuperFieldInfo;
    final ArrayList<JReferenceFieldInfo> referenceFieldInfos = new ArrayList<>();
    final String path;

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Constructor.
     *
     * @param jdb {@link JSimpleDB} against which to resolve object and field names
     * @param startType starting Java type for the path
     * @param path dot-separated path of zero or more reference fields, followed by a target field
     * @param lastIsSubField true if the last field can be a complex sub-field but not a complex field, false for the reverse,
     *  or null for don't care
     * @throws IllegalArgumentException if {@code jdb}, {@code startType}, or {@code path} is null
     * @throws IllegalArgumentException if {@code path} is invalid
     */
    ReferencePath(JSimpleDB jdb, Class<?> startType, String path, Boolean lastIsSubField) {

        // Sanity check
        Preconditions.checkArgument(jdb != null, "null jdb");
        Preconditions.checkArgument(startType != null, "null startType");
        Preconditions.checkArgument(path != null, "null path");
        final String errorPrefix = "invalid path `" + path + "': ";
        if (path.length() == 0)
            throw new IllegalArgumentException(errorPrefix + "path is empty");
        this.startType = startType;
        this.path = path;

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
        Class<?> currentType = this.startType;
        JFieldInfo fieldInfo = null;
        JComplexFieldInfo superFieldInfo = null;
        if (this.log.isTraceEnabled()) {
            this.log.trace("RefPath: START: startType=" + this.startType + " path=" + fieldNames
              + " lastIsSubField=" + lastIsSubField);
        }

        // Parse field names
        final ArrayList<Integer> storageIds = new ArrayList<>();
        assert !fieldNames.isEmpty();
        while (!fieldNames.isEmpty()) {
            final String fieldName = fieldNames.removeFirst();
            String description = "field `" + fieldName + "' in type " + currentType;

            // Get explicit storage ID, if any
            final int hash = fieldName.indexOf('#');
            int explicitStorageId = 0;
            final String searchName;
            if (hash != -1) {
                try {
                    explicitStorageId = Integer.parseInt(fieldName.substring(hash + 1));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(errorPrefix + "invalid field name `" + fieldName + "'");
                }
                searchName = fieldName.substring(0, hash);
            } else
                searchName = fieldName;

            // Logging
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath: [" + searchName + "] currentType=" + currentType + " storageId=" + explicitStorageId);

            // Find all JFields matching 'fieldName' in some JClass whose type matches 'typeToken'
            final HashMap<JClass<?>, JField> matchingFields = new HashMap<>();
            for (JClass<?> jclass : jdb.jclasses.values()) {
                if (!currentType.isAssignableFrom(jclass.type))
                    continue;
                final JField jfield = jclass.jfieldsByName.get(searchName);
                if (jfield == null)
                    continue;
                if (explicitStorageId != 0 && jfield.storageId != explicitStorageId)
                    continue;
                matchingFields.put(jclass, jfield);
            }

            // Logging
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath: matching fields: " + matchingFields);

            // Check matching fields and verify they all have the same storage ID
            if (matchingFields.isEmpty()) {
                throw new IllegalArgumentException(errorPrefix + "there is no field named `" + searchName + "'"
                  + (explicitStorageId != 0 ? " with storage ID " + explicitStorageId : "")
                  + " in (any sub-type of) " + currentType);
            }
            final int fieldStorageId = matchingFields.values().iterator().next().storageId;
            for (JField jfield : matchingFields.values()) {
                if (jfield.storageId != fieldStorageId) {
                    throw new IllegalArgumentException(errorPrefix + "there are multiple, non-equal fields named `"
                      + fieldName + "' in sub-types of type " + currentType);
                }
            }
            fieldInfo = jdb.getJFieldInfo(fieldStorageId, JFieldInfo.class);

            // Get common supertype of all types containing the field
            currentType = Util.findLowestCommonAncestorOfClasses(
              Iterables.transform(matchingFields.keySet(), new JClassTypeFunction())).getRawType();

            // Logging
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath: updated currentType=" + currentType + " fieldType=" + fieldInfo.getTypeToken(currentType));

            // Handle complex fields
            superFieldInfo = null;
            if (fieldInfo instanceof JComplexFieldInfo) {
                final JComplexFieldInfo complexFieldInfo = (JComplexFieldInfo)fieldInfo;

                // Last field?
                if (fieldNames.isEmpty()) {
                    if (lastIsSubField != null && lastIsSubField) {
                        throw new IllegalArgumentException(errorPrefix + "path may not end on complex " + description
                          + "; a sub-field of must be specified");
                    }
                    break;
                }
                superFieldInfo = complexFieldInfo;

                // Get sub-field
                final String subFieldName = fieldNames.removeFirst();
                description = "sub-field `" + subFieldName + "' of complex " + description;
                try {
                    fieldInfo = complexFieldInfo.getSubFieldInfo(subFieldName);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(errorPrefix + "invalid " + description + ": " + e.getMessage(), e);
                }

                // Update matching fields to match sub-field
                for (Map.Entry<JClass<?>, JField> entry : matchingFields.entrySet())
                    entry.setValue(((JComplexField)entry.getValue()).getSubField(subFieldName));

                // Logging
                if (this.log.isTraceEnabled())
                    this.log.trace("RefPath: [" + searchName + "." + subFieldName + "] new matching fields=" + matchingFields);
            }

            // Last field?
            if (fieldNames.isEmpty()) {
                if (superFieldInfo != null && Boolean.FALSE.equals(lastIsSubField)) {
                    throw new IllegalArgumentException(errorPrefix + "path may not end on " + description
                      + "; specify the complex field itself instead");
                }
                break;
            }

            // Field is not last, so it must be a reference field
            if (!(fieldInfo instanceof JReferenceFieldInfo))
                throw new IllegalArgumentException(errorPrefix + description + " is not a reference field");
            final JReferenceFieldInfo referenceFieldInfo = (JReferenceFieldInfo)fieldInfo;

            // Add reference field to the reference field list
            this.referenceFieldInfos.add(referenceFieldInfo);

            // Advance through the reference, using the narrowest type possible
            currentType = Util.findLowestCommonAncestor(Iterables.transform(
               Iterables.transform(matchingFields.values(), new CastFunction<JReferenceField>(JReferenceField.class)),
              new JReferenceFieldTypeFunction())).getRawType();

            // Logging
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath: step through reference, new currentType=" + currentType);
        }

        // Done
        this.targetType = currentType;
        this.targetFieldInfo = fieldInfo;
        this.targetSuperFieldInfo = superFieldInfo;

        // Logging
        if (this.log.isTraceEnabled()) {
            this.log.trace("RefPath: DONE: targetType=" + this.targetType + " targetFieldInfo=" + this.targetFieldInfo
              + " targetSuperFieldInfo=" + this.targetSuperFieldInfo + " targetFieldType=" + this.getTargetFieldType()
              + " references=" + this.referenceFieldInfos);
        }
    }

    /**
     * Get the Java type of the object at which this path starts.
     *
     * <p>
     * If there are zero {@linkplain #getReferenceFields reference fields} in this path, then this will
     * equal the {@linkplain #getTargetType target object type}, or possibly a super-type if the target
     * field exists only in a sub-type.
     * </p>
     *
     * @return the Java type at which this reference path starts
     */
    public Class<?> getStartType() {
        return this.startType;
    }

    /**
     * Get the Java type of the object at which this path ends.
     *
     * <p>
     * If there are zero {@linkplain #getReferenceFields reference fields} in this path, then this will
     * equal the Java type of the {@linkplain #getStartType starting object type}, or possibly a sub-type
     * if the target field exists only in a sub-type.
     * </p>
     *
     * @return the Java type at which this reference path ends
     */
    public Class<?> getTargetType() {
        return this.targetType;
    }

    /**
     * Get the Java type corresponding to the field at which this path ends.
     *
     * @return the type of the field at which this reference path ends
     */
    public TypeToken<?> getTargetFieldType() {
        return this.targetFieldInfo.getTypeToken(this.targetType);
    }

    /**
     * Get the storage ID associated with the target field in the {@linkplain #getTargetType target object type}.
     *
     * <p>
     * This is just the storage ID of the last field in the path.
     * </p>
     *
     * @return the storage ID of the field at which this reference path ends
     */
    public int getTargetField() {
        return this.targetFieldInfo.storageId;
    }

    /**
     * Get the storage ID associated with the complex field containing the {@linkplain #getTargetField target field}
     * a sub-field, in the case that the target field is a sub-field of a complex field.
     *
     * @return target field's complex super-field storage ID, or zero if the target field is not a complex sub-field
     */
    public int getTargetSuperField() {
        return this.targetSuperFieldInfo != null ? this.targetSuperFieldInfo.storageId : 0;
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
        final int[] storageIds = new int[this.referenceFieldInfos.size()];
        for (int i = 0; i < storageIds.length; i++)
            storageIds[i] = this.referenceFieldInfos.get(i).storageId;
        return storageIds;
    }

    /**
     * Get the {@link String} form of the path associated with this instance.
     */
    @Override
    public String toString() {
        return this.path;
    }

// Functions

    private static class JReferenceFieldTypeFunction implements Function<JReferenceField, TypeToken<?>> {

        @Override
        public TypeToken<?> apply(JReferenceField jfield) {
            return jfield.typeToken;
        }
    }

    private static class JClassTypeFunction implements Function<JClass<?>, Class<?>> {

        @Override
        public Class<?> apply(JClass<?> jclass) {
            return jclass.type;
        }
    }
}

