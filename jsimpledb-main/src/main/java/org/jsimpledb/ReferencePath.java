
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.reflect.TypeToken;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
 * &#64;JSimpleClass
 * public class Person {
 *
 *     &#64;JSimpleSetField
 *     public abstract Set&lt;Person&gt; <b>getFriends</b>();
 *
 *     &#64;OnChange("friends.element.<b>name</b>")
 *     private void friendNameChanged(SimpleFieldChange&lt;NamedPerson, String&gt; change) {
 *         // ... do whatever
 *     }
 * }
 *
 * &#64;JSimpleClass
 * public class NamedPerson extends Person {
 *
 *     &#64;JSimpleField
 *     public abstract String <b>getName</b>();
 *     public abstract void setName(String name);
 * }
 * </pre>
 * Here the path {@code "friends.element.name"} is technically incorrect because {@code "friends.element"}
 * has type {@code Person}, and {@code "name"} is a field of {@code NamedPerson}, not {@code Person}. However, this will
 * still work as long as there is no ambiguity, i.e., in this example, there are no other sub-types of {@code Person}
 * with a field named {@code "name"}. Note also in the example above the {@link org.jsimpledb.change.SimpleFieldChange}
 * parameter to the method {@code friendNameChanged()} necessarily has generic type {@code NamedPerson}, not {@code Person}.
 *
 * <p>
 * Note also that a field may have different types in two different classes (as long as it's not indexed in both classes),
 * in which case type of the "target field" is effectively the lowest common ancestor of the field's types (in the worst
 * case, {@link Object}).
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
    final Set<TypeToken<?>> targetFieldTypes;
    final int targetFieldStorageId;
    final int targetSuperFieldStorageId;
    final int[] referenceFieldStorageIds;
    final Set<Cursor> cursors;
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

        // Debug
        if (this.log.isTraceEnabled())
            this.log.trace("RefPath: START startType=" + startType + " path=\"" + path + "\" lastIsSubField=" + lastIsSubField);

        // Split the path into field names
        final ArrayDeque<String> fieldNames = new ArrayDeque<>();
        while (true) {
            final int dot = path.indexOf('.');
            if (dot == -1) {
                if (path.length() == 0)
                    throw new IllegalArgumentException(errorPrefix + "path ends in `.'");
                fieldNames.add(path);
                break;
            }
            if (dot == 0)
                throw new IllegalArgumentException(errorPrefix + "path contains an empty path component");
            fieldNames.add(path.substring(0, dot));
            path = path.substring(dot + 1);
        }

        // Debug
        if (this.log.isTraceEnabled())
            this.log.trace("RefPath: fieldNames=" + fieldNames);

        // Initialize loop state
        final HashSet<Cursor> remainingCursors = new HashSet<>();
        jdb.getJClasses(this.startType).stream()
          .map(jclass -> new Cursor(jclass, fieldNames))
          .forEach(remainingCursors::add);

        // Recursively advance cursors
        IllegalArgumentException error = null;
        final HashSet<Cursor> completedCursors = new HashSet<>();
        while (!remainingCursors.isEmpty()) {

            // Debug
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath: remainingCursors=" + remainingCursors);

            // Advance outstanding cursors
            final HashSet<Cursor> outstandingCursors = new HashSet<>(remainingCursors);
            remainingCursors.clear();
            for (Cursor cursor : outstandingCursors) {

                // Try to step through the next field in the path
                try {
                    cursor = cursor.stepThroughField(lastIsSubField);
                } catch (IllegalArgumentException e) {
                    error = e;
                    continue;
                }

                // Debug
                if (this.log.isTraceEnabled()) {
                    this.log.trace("RefPath: after stepThroughField(), cursor="
                      + cursor + ", end-of-path=" + !cursor.hasMoreFieldNames());
                }

                // Have we reached the end of the path?
                if (!cursor.hasMoreFieldNames()) {
                    completedCursors.add(cursor);
                    continue;
                }

                // Debug
                if (this.log.isTraceEnabled())
                    this.log.trace("RefPath: invoking stepThroughReference() on " + cursor);

                // "Dereference" the reference field, possibly branching to create multiple new cursors
                final Set<Cursor> referenceCursors;
                try {
                    referenceCursors = cursor.stepThroughReference(jdb);
                } catch (IllegalArgumentException e) {
                    error = e;
                    continue;
                }

                // Debug
                if (this.log.isTraceEnabled())
                    this.log.trace("RefPath: stepThroughReference() returned " + referenceCursors);
                remainingCursors.addAll(referenceCursors);
            }
        }

        // Check for error
        if (this.log.isTraceEnabled())
            this.log.trace("RefPath: remainingCursors=" + remainingCursors);
        if (completedCursors.isEmpty())
            throw error;

        // Check that all cursors took the same path
        final ArrayList<Integer> referenceFieldList = completedCursors.iterator().next().getReferenceFields();
        for (Cursor cursor : completedCursors) {
            if (!cursor.getReferenceFields().equals(referenceFieldList)) {
                throw new IllegalArgumentException(errorPrefix
                  + "path is ambiguous due to traversal of fields with different types");
            }
        }

        // Sanity check target field has a consistent storage ID and parent storage ID
        final Set<Integer> targetFieldStorageIds = completedCursors.stream()
          .map(Cursor::getField)
          .map(JField::getStorageId)
          .collect(Collectors.toSet());
        final Set<Integer> targetSuperFieldStorageIds = completedCursors.stream()
          .map(Cursor::getSuperField)
          .map(sf -> sf != null ? sf.storageId : 0)
          .collect(Collectors.toSet());
        if (targetFieldStorageIds.size() != 1 || targetSuperFieldStorageIds.size() != 1) {
            throw new IllegalArgumentException(errorPrefix + "the target field `" + fieldNames.pollLast()
              + "' is ambiguous: " + completedCursors.stream().map(Cursor::getField).collect(Collectors.toSet()));
        }

        // Minimize our set of target object types
        final Set<Class<?>> allTargetTypes = completedCursors.stream()
          .map(Cursor::getJClass)
          .map(JClass::getType)
          .collect(Collectors.toSet());
        final HashSet<Class<?>> minimalTargetTypes = new HashSet<>();
        for (TypeToken<?> typeToken : Util.findLowestCommonAncestorsOfClasses(allTargetTypes))
            minimalTargetTypes.add(typeToken.getRawType());

        // Calculate all possible target field types
        final Set<TypeToken<?>> targetFieldTypesSet = completedCursors.stream()
          .map(Cursor::getField)
          .map(JField::getTypeToken)
          .collect(Collectors.toSet());

        // Done
        this.targetTypes = Collections.unmodifiableSet(minimalTargetTypes);
        this.targetFieldTypes = Collections.unmodifiableSet(targetFieldTypesSet);
        this.targetFieldStorageId = targetFieldStorageIds.iterator().next();
        this.targetSuperFieldStorageId = targetSuperFieldStorageIds.iterator().next();
        this.referenceFieldStorageIds = Ints.toArray(referenceFieldList);
        this.cursors = completedCursors;

        // Logging
        if (this.log.isTraceEnabled()) {
            this.log.trace("RefPath: DONE: targetTypes=" + this.targetTypes + " targetFieldStorageId=" + this.targetFieldStorageId
              + " targetSuperFieldStorageId=" + this.targetSuperFieldStorageId + " targetFieldTypes=" + this.targetFieldTypes
              + " references=" + referenceFieldList + " cursors=" + this.cursors);
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
        return this.targetFieldTypes;
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
        return this.targetFieldStorageId;
    }

    /**
     * Get the storage ID associated with the complex field containing the {@linkplain #getTargetField target field}
     * a sub-field, in the case that the target field is a sub-field of a complex field.
     *
     * @return target field's complex super-field storage ID, or zero if the target field is not a complex sub-field
     */
    public int getTargetSuperField() {
        return this.targetSuperFieldStorageId;
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
        return this.referenceFieldStorageIds.clone();
    }

    /**
     * Get the {@link String} form of the path associated with this instance.
     */
    @Override
    public String toString() {
        return this.path;
    }

// Cursor

    /**
     * A cursor position in a partially parsed reference path.
     *
     * <p>
     * An instance points to a {@link JClass}, and optionally to a {@link JField} within that class.
     * It has a list of references it has already traversed, and a list of field names it has yet to traverse.
     * Traversing a field involves two steps: (a) finding the field via {@link #stepThroughField},
     * and (if not done yet) (b) dereferencing that (presumably reference) field via {@link #stepThroughReference}.
     * Note prior to step (a), {@code this.jfield} is the field we are going to step through; prior to
     * step (b), {@code this.jfield} is the field we just dereferenced in the previous {@link JClass}.
     *
     * <p>
     * Instances are immutable.
     */
    static final class Cursor {

        private final Logger log = LoggerFactory.getLogger(this.getClass());
        private final ArrayList<Integer> referenceFields = new ArrayList<>();
        private final JClass<?> jclass;
        private final JField jfield;
        private final ArrayDeque<String> fieldNames;
        private final boolean steppedThroughField;

        private Cursor(JClass<?> jclass, ArrayDeque<String> fieldNames) {
            this(new ArrayList<>(), jclass, null, fieldNames, false);
        }

        private Cursor(ArrayList<Integer> referenceFields, JClass<?> jclass,
          JField jfield, ArrayDeque<String> fieldNames, boolean steppedThroughField) {
            this.referenceFields.addAll(referenceFields);
            this.jclass = jclass;
            this.jfield = jfield;
            this.fieldNames = fieldNames.clone();
            this.steppedThroughField = steppedThroughField;
        }

        public ArrayList<Integer> getReferenceFields() {
            return this.referenceFields;
        }

        public JClass<?> getJClass() {
            return this.jclass;
        }

        public JField getField() {
            return this.jfield;
        }

        public JComplexField getSuperField() {
            return this.jfield instanceof JSimpleField ? ((JSimpleField)this.jfield).getParentField() : null;
        }

        public boolean hasMoreFieldNames() {
            return !this.fieldNames.isEmpty();
        }

        public boolean isSteppedThroughField() {
            return this.steppedThroughField;
        }

        /**
         * Step through the next field name and return the resulting cursor.
         *
         * @param lastIsSubField true if the last field can be a complex sub-field but not a complex field, false for the reverse,
         *  or null for don't care
         * @return resulting cursor
         * @throws IllegalArgumentException if step is bogus
         */
        public Cursor stepThroughField(Boolean lastIsSubField) {

            // Sanity check
            Preconditions.checkArgument(!this.fieldNames.isEmpty(), "empty reference path");
            Preconditions.checkState(!this.steppedThroughField, "already stepped through field");

            // Get field name
            final ArrayDeque<String> remainingFieldNames = this.fieldNames.clone();
            final String fieldName = remainingFieldNames.removeFirst();
            String description = "field `" + fieldName + "' in " + this.jclass.getType();

            // Parse explicit storage ID, if any
            final int hash = fieldName.indexOf('#');
            int explicitStorageId = 0;
            final String searchName;
            if (hash != -1) {
                try {
                    explicitStorageId = Integer.parseInt(fieldName.substring(hash + 1));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("invalid field name `" + fieldName + "'");
                }
                searchName = fieldName.substring(0, hash);
            } else
                searchName = fieldName;

            // Logging
            if (this.log.isTraceEnabled()) {
                this.log.trace("RefPath.stepThroughField(): cursor=" + this
                  + " searchName=" + searchName + " storageId=" + explicitStorageId);
            }

            // Find the JField matching 'fieldName' in jclass
            JField matchingField = this.jclass.jfieldsByName.get(searchName);
            if (matchingField == null || (explicitStorageId != 0 && matchingField.storageId != explicitStorageId)) {
                throw new IllegalArgumentException("there is no field named `" + searchName + "'"
                  + (explicitStorageId != 0 ? " with storage ID " + explicitStorageId : "") + " in " + this.jclass.getType());
            }

            // Logging
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath.stepThroughField(): found field " + matchingField + " in " + this.jclass.getType());

            // Handle complex fields
            JComplexField superField = null;
            if (matchingField instanceof JComplexField) {
                superField = (JComplexField)matchingField;

                // Logging
                if (this.log.isTraceEnabled())
                    this.log.trace("RefPath.stepThroughField(): field is a complex field");

                // Last field?
                if (remainingFieldNames.isEmpty()) {

                    // Verify it's OK to end on a complex field
                    if (Boolean.TRUE.equals(lastIsSubField)) {
                        final StringBuilder buf = new StringBuilder();
                        for (JSimpleField subField : superField.getSubFields()) {
                            if (buf.length() == 0) {
                                buf.append("path may not end on complex ")
                                  .append(description)
                                  .append("; a sub-field must be specified (e.g., ");
                            } else
                                buf.append(" or ");
                            buf.append('`').append(matchingField.name).append('.').append(subField.name).append('\'');
                        }
                        buf.append(")");
                        throw new IllegalArgumentException(buf.toString());
                    }

                    // Done
                    final Cursor result = new Cursor(this.referenceFields, this.jclass, matchingField, remainingFieldNames, true);
                    if (this.log.isTraceEnabled())
                        this.log.trace("RefPath.stepThroughField(): ended on complex field; result=" + result);
                    return result;
                }

                // Get the specified sub-field
                final String subFieldName = remainingFieldNames.removeFirst();
                description = "sub-field `" + subFieldName + "' of complex " + description;
                try {
                    matchingField = superField.getSubField(subFieldName);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("invalid " + description + ": " + e.getMessage(), e);
                }

                // Logging
                if (this.log.isTraceEnabled()) {
                    this.log.trace("RefPath.stepThroughField(): also stepping through sub-field ["
                      + searchName + "." + subFieldName + "] to reach " + matchingField);
                }
            } else if (this.log.isTraceEnabled())
                this.log.trace("RefPath.stepThroughField(): field is a simple field");

            // Verify it's OK to end on a complex sub-field (if that's what happened)
            if (remainingFieldNames.isEmpty() && superField != null && Boolean.FALSE.equals(lastIsSubField)) {
                throw new IllegalArgumentException("path may not end on " + description
                  + "; instead, specify the complex field itself");
            }

            // Done
            final Cursor result = new Cursor(this.referenceFields, this.jclass, matchingField, remainingFieldNames, true);
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath.stepThroughField(): result=" + result);
            return result;
        }

        /**
         * Step through the current reference field to the referred-to types.
         */
        public Set<Cursor> stepThroughReference(JSimpleDB jdb) {

            // Logging
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath.stepThroughReference(): this=" + this);

            // Sanity check
            Preconditions.checkState(this.steppedThroughField, "have not yet stepped through field");
            assert this.jfield != null;
            assert this.jfield.parent == this.jclass
              || (this.jfield instanceof JSimpleField && ((JSimpleField)this.jfield).getParentField().parent == this.jclass);
            Preconditions.checkArgument(this.jfield instanceof JReferenceField, this.jfield + " is not a reference field");

            // Append reference field to list
            final ArrayList<Integer> newReferenceFields = new ArrayList<>(this.referenceFields.size() + 1);
            newReferenceFields.addAll(this.referenceFields);
            newReferenceFields.add(this.jfield.storageId);

            // Advance through the reference
            final Class<?> referencedType = ((JReferenceField)this.jfield).typeToken.getRawType();
            if (this.log.isTraceEnabled()) {
                this.log.trace("RefPath.stepThroughReference(): referenced type is "
                  + referencedType + " -> " + jdb.getJClasses(referencedType));
            }
            final HashSet<Cursor> cursors = new HashSet<>();
            for (JClass<?> referencedJClass : jdb.getJClasses(referencedType))
                cursors.add(new Cursor(newReferenceFields, referencedJClass, jfield, this.fieldNames, false));

            // Done
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath.stepThroughReference(): result=" + cursors);
            return cursors;
        }

        /**
         * Append the given field name.
         */
        public Cursor appendField(String fieldName) {
            final ArrayDeque<String> newFieldNames = this.fieldNames.clone();
            newFieldNames.add(fieldName);
            return new Cursor(this.referenceFields, this.jclass, this.jfield, newFieldNames, this.steppedThroughField);
        }

    // Object

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final Cursor that = (Cursor)obj;
            return this.referenceFields.equals(that.referenceFields)
              && this.jclass.equals(that.jclass)
              && Objects.equals(this.jfield, that.jfield);
        }

        @Override
        public int hashCode() {
            return this.referenceFields.hashCode()
              ^ this.jclass.hashCode()
              ^ Objects.hashCode(this.jfield);
        }

        @Override
        public String toString() {
            return "Cursor"
              + "[jclass=" + this.jclass
              + (this.jfield != null ? ",jfield=" + this.jfield : "")
              + (!this.fieldNames.isEmpty() ? ",fieldNames=" + this.fieldNames : "")
              + (!this.referenceFields.isEmpty() ? ",refs=" + this.referenceFields : "")
              + ",steppedThroughField=" + this.steppedThroughField
              + "]";
        }
    }
}

