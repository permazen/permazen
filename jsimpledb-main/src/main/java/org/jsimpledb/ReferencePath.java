
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
 *
 * <p>
 * For example, path {@code "parent.age"} starting from object type {@code Person} might refer to the age of
 * the parent of a {@code Person}.
 *
 * <p>
 * When a complex field appears in a reference path, both the name of the complex field and the specific sub-field
 * being traversed should appear, e.g., {@code "mymap.key.somefield"}. For set and list fields, the (only) sub-field is
 * named {@code "element"}, while for map fields the sub-fields are named {@code "key"} and {@code "value"}.
 *
 * <p>
 * <b>Fields of Sub-Types</b>
 *
 * <p>
 * In some cases, a field may not exist in a Java object type, but it does exist in a some sub-type of that type. For example:
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
 *
 * <p>
 * Reference paths are created via {@link JSimpleDB#parseReferencePath JSimpleDB.parseReferencePath()}.
 *
 * @see JSimpleDB#parseReferencePath JSimpleDB.parseReferencePath()
 */
public class ReferencePath {

    final Class<?> startType;
    final Set<Class<?>> targetTypes;
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
        final HashSet<Class<?>> currentTypes = new HashSet<>(3);
        currentTypes.add(this.startType);
        JFieldInfo fieldInfo = null;
        JComplexFieldInfo superFieldInfo = null;
        if (this.log.isTraceEnabled()) {
            this.log.trace("RefPath: START: startType=" + this.startType + " path=" + fieldNames
              + " lastIsSubField=" + lastIsSubField);
        }

        // Parse field names
        assert !fieldNames.isEmpty();
        while (!fieldNames.isEmpty()) {
            final String fieldName = fieldNames.removeFirst();
            String description = "field `" + fieldName + "' in type "
              + Util.findLowestCommonAncestorOfClasses(currentTypes).getRawType();

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
                this.log.trace("RefPath: [" + searchName + "] currentTypes=" + currentTypes + " storageId=" + explicitStorageId);

            // Find all JFields matching 'fieldName' in some JClass whose type matches some type in 'currentTypes'
            final HashMap<JClass<?>, JField> matchingFields = new HashMap<>();
            final HashSet<JClass<?>> currentTypeJClasses = new HashSet<>(currentTypes.size());
            for (Class<?> currentType : currentTypes)
                currentTypeJClasses.addAll(jdb.getJClasses(currentType));
            for (JClass<?> jclass : currentTypeJClasses) {
                final JField jfield = jclass.jfieldsByName.get(searchName);
                if (jfield == null)
                    continue;
                if (explicitStorageId != 0 && jfield.storageId != explicitStorageId)
                    continue;
                assert !matchingFields.containsKey(jclass) || jfield.equals(matchingFields.get(jclass));
                matchingFields.put(jclass, jfield);
            }

            // Logging
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath: matching fields: " + matchingFields);

            // Check matching fields and verify they all have the same storage ID
            if (matchingFields.isEmpty()) {
                throw new IllegalArgumentException(errorPrefix + "there is no field named `" + searchName + "'"
                  + (explicitStorageId != 0 ? " with storage ID " + explicitStorageId : "")
                  + " in (any sub-type of) " + Util.findLowestCommonAncestorOfClasses(currentTypes).getRawType());
            }
            final int fieldStorageId = matchingFields.values().iterator().next().storageId;
            for (JField jfield : matchingFields.values()) {
                if (jfield.storageId != fieldStorageId) {
                    throw new IllegalArgumentException(errorPrefix + "there are multiple, non-equal fields named `"
                      + fieldName + "' in sub-types of type " + Util.findLowestCommonAncestorOfClasses(currentTypes).getRawType());
                }
            }
            fieldInfo = jdb.getJFieldInfo(fieldStorageId, JFieldInfo.class);

            // Get types containing the field
            currentTypes.clear();
            matchingFields.keySet().stream()
              .map(jclass -> jclass.type)
              .forEach(currentTypes::add);

            // Logging
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath: updated currentTypes=" + currentTypes + " fieldInfo=" + fieldInfo);

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

            // Advance through the reference
            currentTypes.clear();
            for (JField jfield : matchingFields.values())
                currentTypes.add(((JReferenceField)jfield).typeToken.getRawType());

            // Logging
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath: step through reference, new currentTypes=" + currentTypes);
        }

        // Minimize our set of target types
        final HashSet<Class<?>> minimalTargetTypes = new HashSet<>();
        for (TypeToken<?> typeToken : Util.findLowestCommonAncestorsOfClasses(currentTypes))
            minimalTargetTypes.add(typeToken.getRawType());

        // Done
        this.targetTypes = Collections.unmodifiableSet(minimalTargetTypes);
        this.targetFieldInfo = fieldInfo;
        this.targetSuperFieldInfo = superFieldInfo;

        // Logging
        if (this.log.isTraceEnabled()) {
            this.log.trace("RefPath: DONE: targetTypes=" + this.targetTypes + " targetTypes=" + this.targetTypes
              + " targetFieldInfo=" + this.targetFieldInfo + " targetSuperFieldInfo=" + this.targetSuperFieldInfo
              + " targetFieldTypes=" + this.getTargetFieldTypes() + " references=" + this.referenceFieldInfos);
        }
    }

    /**
     * Get the Java type of the object at which this path starts.
     *
     * <p>
     * If there are zero {@linkplain #getReferenceFields reference fields} in this path, then this will
     * equal the {@linkplain #getTargetType target object type}, or possibly a super-type if the target
     * field exists only in a sub-type.
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
     * The returned type will be as narrow as possible while still including all possibilities, but note that it's
     * possible for there to be multiple candidates for the "target type", none of which is a sub-type of any other.
     * To retrieve all such target types, use {@link #getTargetTypes}; this method just invokes
     * {@link Util#findLowestCommonAncestorOfClasses Util.findLowestCommonAncestorOfClasses()} on the result.
     *
     * @return the Java type at which this reference path ends
     */
    public Class<?> getTargetType() {
        return Util.findLowestCommonAncestorOfClasses(this.targetTypes).getRawType();
    }

    /**
     * Get the possible Java types of the object at which this path ends.
     *
     * <p>
     * If there are zero {@linkplain #getReferenceFields reference fields} in this path, then this method will
     * return only the Java type of the {@linkplain #getStartType starting object type}, or possibly a sub-type
     * if the target field exists only in a sub-type.
     *
     * <p>
     * The returned type(s) will be maximally narrow. The set will contain only one element if a unique such
     * type exists, otherwise it will contain multiple mutually incompatible supertypes of the object types
     * at which this path ends.
     *
     * @return the Java type(s) at which this reference path ends
     */
    public Set<Class<?>> getTargetTypes() {
        return this.targetTypes;
    }

    /**
     * Get the Java type(s) corresponding to the field at which this path ends.
     *
     * <p>
     * The returned type(s) will be maximally narrow. The set will contain only one element if a unique such
     * type exists, otherwise it will contain multiple mutually incompatible supertypes of the object types
     * at which this path ends. The latter case can only occur when the field is a reference field, and there
     * are multiple Java model classes compatible with the field's type.
     *
     * @return the type of the field at which this reference path ends
     */
    public Set<TypeToken<?>> getTargetFieldTypes() {
        final HashSet<TypeToken<?>> targetFieldTypes = new HashSet<>(this.targetTypes.size());
        for (Class<?> targetType : this.targetTypes)
            targetFieldTypes.addAll(this.targetFieldInfo.getTypeTokens(targetType));
        return targetFieldTypes;
    }

    /**
     * Get the storage ID associated with the target field in the {@linkplain #getTargetType target object type}.
     *
     * <p>
     * This is just the storage ID of the last field in the path.
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
     *
     * <p>
     * Otherwise, the first field is a field in the {@linkplain #getStartType starting object type} and
     * the last field is field in some object type that refers to the {@linkplain #getTargetType target object type}.
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
}

