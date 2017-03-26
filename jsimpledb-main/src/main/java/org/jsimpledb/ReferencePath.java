
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.reflect.TypeToken;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.jsimpledb.schema.SchemaObjectType;
import org.jsimpledb.util.ParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reference paths.
 *
 * <p>
 * A reference path is a {@link String} specifying a path from some implicit <em>starting object type</em>, through zero
 * or more reference fields, ending up at some <em>target object type(s)</em>.
 * In other words, given some starting object(s), a reference path takes you through a path of references to the target object(s).
 *
 * <p>
 * The starting object type is always implicit from the context in which the path is used. The {@link String} path describes
 * the reference fields, i.e., the "steps" in the path that travel from the starting object type to the target object type.
 * The steps are separated by period ({@code "."}) characters.
 *
 * <p>
 * The reference fields in the path may be traversed in either the forward or inverse direction:
 * to traverse a field in the forward direction, specify name of the field; to traverse a field in the inverse direction,
 * specify the name of the referring field and its containing type using the syntax {@code ^Type:field^}.
 *
 * <p>
 * For complex fields, specify the field with sub-field: for {@link Set} and {@link java.util.List} fields, the sub-field is
 * always {@code "element"}, while for {@link java.util.Map} fields the sub-field is either {@code "key"} or {@code "value"}.
 *
 * <p>
 * A reference path may optionally have a <b>target field</b> appended to the end of the path. If so, the target object types
 * are restricted to those types containing the target field. Note that whether there is a target field must be known at
 * the time the path is parsed: for example, the path {@code "parent.parent"} in the context of a {@code Child} refers to the
 * {@code "parent"} field of the child's parent if the path has a target field, but it refers to the child's grandparent if
 * the path does not have a target field.
 *
 * <p>
 * Considering the following model classes:
 *
 * <pre>
 * &#64;JSimpleClass
 * public class Employee {
 *
 *     public abstract String getName();
 *     public abstract void setName(String name);
 *
 *     public abstract Employee getManager();
 *     public abstract void setManager(Employee manager);
 *
 *     public abstract Set&lt;Asset&gt; getAssets();
 * }
 *
 * &#64;JSimpleClass
 * public class Asset {
 *
 *     public abstract int getAssetId();
 *     public abstract void setAssetId(int assetId);
 *
 *     public abstract String getName();
 *     public abstract void setName(String name);
 * }
 * </pre>
 *
 * Then these paths have the following meanings:
 *
 * <div style="margin-left: 20px;">
 * <table border="1" cellpadding="3" cellspacing="0" summary="Reference Path Examples">
 * <tr style="bgcolor:#ccffcc">
 *  <th align="left">Start&nbsp;Type</th>
 *  <th align="left">Path</th>
 *  <th align="left">Has Target Field?</th>
 *  <th align="left">Description</th>
 * </tr>
 * <tr>
 *  <td>{@code Employee}</td>
 *  <td>{@code ""}</td>
 *  <td>No</td>
 *  <td>The {@code Employee}</td>
 * </tr>
 * <tr>
 *  <td>{@code Employee}</td>
 *  <td>{@code ""}</td>
 *  <td>Yes</td>
 *  <td>Invalid - no target field specified</td>
 * </tr>
 * <tr>
 *  <td>{@code Employee}</td>
 *  <td>{@code "name"}</td>
 *  <td>Yes</td>
 *  <td>The {@code Employee}'s name</td>
 * </tr>
 * <tr>
 *  <td>{@code Employee}</td>
 *  <td>{@code "name"}</td>
 *  <td>No</td>
 *  <td>Invalid - {@code "name"} is not a reference field</td>
 * </tr>
 * <tr>
 *  <td>{@code Asset}</td>
 *  <td>{@code "name"}</td>
 *  <td>Yes</td>
 *  <td>The {@code Asset}'s name</td>
 * </tr>
 * <tr>
 *  <td>{@code Object}</td>
 *  <td>{@code "name"}</td>
 *  <td>Yes</td>
 *  <td>The {@link Employee}'s or {@link Asset}'s name</td>
 * </tr>
 * <tr>
 *  <td>{@code Employee}</td>
 *  <td>{@code "manager"}</td>
 *  <td>No</td>
 *  <td>The {@code Employee}'s manager</td>
 * </tr>
 * <tr>
 *  <td>{@code Employee}</td>
 *  <td>{@code "manager"}</td>
 *  <td>Yes</td>
 *  <td>The {@code "manager"} field of the {@code Employee}</td>
 * </tr>
 * <tr>
 *  <td>{@code Employee}</td>
 *  <td>{@code "manager.name"}</td>
 *  <td>Yes</td>
 *  <td>The {@code Employee}'s manager's name</td>
 * </tr>
 * <tr>
 *  <td>{@code Employee}</td>
 *  <td>{@code "^Employee:manager^.name"}</td>
 *  <td>Yes</td>
 *  <td>The names of all of the {@code Employee}'s direct reports</td>
 * </tr>
 * <tr>
 *  <td>{@code Employee}</td>
 *  <td>{@code "manager.assets.description"}</td>
 *  <td>Yes</td>
 *  <td>The descriptions of the {@code Employee}'s manager's assets</td>
 * </tr>
 * <tr>
 *  <td>{@code Employee}</td>
 *  <td>{@code "manager.^Employee:manager^"}</td>
 *  <td>No</td>
 *  <td>All of the {@code Employee}'s manager's direct reports</td>
 * </tr>
 * <tr>
 *  <td>{@code Asset}</td>
 *  <td>{@code "^Employee:assets.element^"}</td>
 *  <td>No</td>
 *  <td>The employee owning the {@code Asset}</td>
 * </tr>
 * <tr>
 *  <td>{@code Asset}</td>
 *  <td>{@code "^Employee:assets.element^"}</td>
 *  <td>Yes</td>
 *  <td>Invalid - no target field specified</td>
 * </tr>
 * <tr>
 *  <td>{@code Asset}</td>
 *  <td>{@code "^Employee:assets.element^}<br/>&nbsp;&nbsp;{@code .manager.^Employee:manager^.asset.assetId"}</td>
 *  <td>Yes</td>
 *  <td>ID's of all {@code Asset}s owned by direct reports of the manager of the {@link Employee} owning the original
 *      {@code Asset}</td>
 * </tr>
 * </table>
 * </div>
 *
 * <p><b>Fields of Sub-Types</b>
 *
 * <p>
 * The same field can appear in multiple types, e.g., {@code "name"} in the example above. The set of all possible
 * object types is recalculated at each step in the reference path, including at the last step, which gives the
 * target object type(s). At each intermediate step, as long as the Java types do not contain incompatible definitions
 * for the named field, the step is valid.
 *
 * <p>
 * In rare cases where multiple sub-types of a common super-type type have fields with the same name but different storage IDs,
 * the storage ID may be explicitly specified as a suffix, for example, {@code "name#123"}.
 *
 * <p>
 * Reference paths are created via {@link JSimpleDB#parseReferencePath JSimpleDB.parseReferencePath()}.
 *
 * @see JSimpleDB#parseReferencePath JSimpleDB.parseReferencePath()
 * @see JTransaction#followReferencePath JTransaction.followReferencePath()
 * @see JTransaction#invertReferencePath JTransaction.invertReferencePath()
 */
public class ReferencePath {

    private static final String IDENT = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";
    private static final String IDENT_ID = IDENT + "(?:#[0-9]+)?";
    private static final String IDENT_ID_1OR2 = IDENT_ID + "(?:\\." + IDENT_ID + ")?";
    private static final String IDENTS = IDENT + "(?:\\." + IDENT + ")*";
    private static final String FWD_STEP = "(" + IDENT_ID + ")";
    private static final String REV_STEP = "\\^((" + IDENTS + "):(" + IDENT_ID_1OR2 + "))\\^";

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
     * @param withTargetField true if this path has a target field
     * @param lastIsSubField true if the last field can be a complex sub-field but not a complex field, false for the reverse,
     *  or null for don't care
     * @throws IllegalArgumentException if {@code jdb}, {@code startType}, or {@code path} is null
     * @throws IllegalArgumentException if {@code path} is invalid
     */
    ReferencePath(JSimpleDB jdb, Class<?> startType, String path, boolean withTargetField, Boolean lastIsSubField) {

        // Sanity check
        Preconditions.checkArgument(jdb != null, "null jdb");
        Preconditions.checkArgument(startType != null, "null startType");
        Preconditions.checkArgument(path != null, "null path");
        final String errorPrefix = "invalid path `" + path + "': ";
        this.startType = startType;
        this.path = path;

        // Debug
        if (this.log.isDebugEnabled()) {
            this.log.debug("RefPath: START startType=" + startType.getName() + " path=\"" + path
              + "\" withTargetField=" + withTargetField + " lastIsSubField=" + lastIsSubField);
        }

        // Split the path into field names
        final ArrayDeque<String> fieldNames = new ArrayDeque<>();
        final ParseContext ctx = new ParseContext(path);
        while (!ctx.isEOF()) {

            // Gobble separator
            if (ctx.getIndex() > 0)
                ctx.expect('.');

            // Parse next step (either forward or reverse)
            Matcher matcher = ctx.tryPattern(FWD_STEP);
            if (matcher == null && (matcher = ctx.tryPattern(REV_STEP)) == null) {
                throw new IllegalArgumentException(errorPrefix + "invalid path starting at `"
                  + ParseContext.truncate(ctx.getInput(), 32) + "'");
            }
            fieldNames.add(matcher.group());
        }

        // Debug
        if (this.log.isDebugEnabled())
            this.log.debug("RefPath: fieldNames=" + fieldNames);

        // Initialize cursors
        final HashSet<Cursor> remainingCursors = new HashSet<>();
        jdb.getJClasses(this.startType).stream()
          .map(jclass -> new Cursor(jclass, fieldNames))
          .forEach(remainingCursors::add);

        // Recursively advance cursors
        IllegalArgumentException error = null;
        final HashSet<Cursor> completedCursors = new HashSet<>();
        while (!remainingCursors.isEmpty()) {

            // Debug
            if (this.log.isDebugEnabled())
                this.log.debug("RefPath: remainingCursors=" + remainingCursors);

            // Advance outstanding cursors
            final HashSet<Cursor> previouslyRemainingCursors = new HashSet<>(remainingCursors);
            remainingCursors.clear();
            for (Cursor cursor : previouslyRemainingCursors) {

                // Debug
                if (this.log.isDebugEnabled())
                    this.log.debug("RefPath: processing remainingCursor " + cursor);

                // Cursors that have no remaining fields, and for which no target field is required, are completed
                if (!withTargetField && !cursor.hasMoreFieldNames()) {
                    if (this.log.isDebugEnabled())
                        this.log.debug("RefPath: remainingCursor " + cursor + " is completed");
                    completedCursors.add(cursor);
                    continue;
                }

                // Try to step through the next field in the path
                final Set<Cursor> newCursors;
                try {
                    newCursors = cursor.stepThroughField(jdb, withTargetField, lastIsSubField);
                } catch (IllegalArgumentException e) {
                    if (this.log.isDebugEnabled())
                        this.log.debug("RefPath: stepThroughField() on " + cursor + " failed: " + e.getMessage());
                    error = e;
                    continue;
                }
                if (this.log.isDebugEnabled())
                    this.log.debug("RefPath: stepThroughField() on " + cursor + " succeeded: newCursors=" + newCursors);

                // Debug
                if (this.log.isDebugEnabled())
                    this.log.debug("RefPath: after stepThroughField(), newCursors=" + newCursors);

                // Remove cursors for which we have reached the end of the path
                for (Iterator<Cursor> i = newCursors.iterator(); i.hasNext(); ) {
                    final Cursor newCursor = i.next();
                    if (withTargetField && !newCursor.hasMoreFieldNames()) {
                        if (newCursor.isReverseStep())                              // target field cannot be a reverse step
                            throw new IllegalArgumentException("Invalid reference path: missing target field");
                        if (this.log.isDebugEnabled())
                            this.log.debug("RefPath: newCursor " + cursor + " is completed");
                        completedCursors.add(newCursor);
                        i.remove();
                    }
                }

                // Debug
                if (this.log.isDebugEnabled())
                    this.log.debug("RefPath: after stepThroughField(), remaining newCursors=" + newCursors);

                // Advance the unfinished cursors through the next reference field
                for (Cursor newCursor : newCursors) {

                    // Debug
                    if (this.log.isDebugEnabled())
                        this.log.debug("RefPath: invoking stepThroughReference() on " + newCursor);

                    // "Dereference" the reference field, possibly branching to create multiple new cursors
                    final Set<Cursor> dereferencedCursors;
                    try {
                        dereferencedCursors = newCursor.stepThroughReference(jdb);
                    } catch (IllegalArgumentException e) {
                        error = e;
                        continue;
                    }

                    // Debug
                    if (this.log.isDebugEnabled())
                        this.log.debug("RefPath: stepThroughReference() returned " + dereferencedCursors);
                    remainingCursors.addAll(dereferencedCursors);
                }
            }
        }

        // Check for error
        if (this.log.isDebugEnabled())
            this.log.debug("RefPath: remainingCursors=" + remainingCursors + " completedCursors=" + completedCursors);
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

        // Get target field info
        if (withTargetField) {

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

            // Calculate all possible target field types
            final Set<TypeToken<?>> targetFieldTypesSet = completedCursors.stream()
              .map(Cursor::getField)
              .map(JField::getTypeToken)
              .collect(Collectors.toSet());

            // Initialize
            this.targetFieldTypes = Collections.unmodifiableSet(targetFieldTypesSet);
            this.targetFieldStorageId = targetFieldStorageIds.iterator().next();
            this.targetSuperFieldStorageId = targetSuperFieldStorageIds.iterator().next();
        } else {
            this.targetFieldTypes = null;
            this.targetFieldStorageId = 0;
            this.targetSuperFieldStorageId = 0;
        }

        // Minimize our set of target object types
        final Set<Class<?>> allTargetTypes = completedCursors.stream()
          .map(Cursor::getJClass)
          .map(JClass::getType)
          .collect(Collectors.toSet());
        final HashSet<Class<?>> minimalTargetTypes = new HashSet<>();
        for (TypeToken<?> typeToken : Util.findLowestCommonAncestorsOfClasses(allTargetTypes))
            minimalTargetTypes.add(typeToken.getRawType());

        // Done
        this.targetTypes = Collections.unmodifiableSet(minimalTargetTypes);
        this.referenceFieldStorageIds = Ints.toArray(referenceFieldList);
        this.cursors = completedCursors;

        // Logging
        if (this.log.isDebugEnabled()) {
            this.log.debug("RefPath: DONE: targetTypes=" + this.targetTypes + " targetFieldStorageId=" + this.targetFieldStorageId
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
     * Get the Java type(s) corresponding to the target field at which this path ends, if any.
     *
     * <p>
     * The returned type(s) will be maximally narrow. The set will contain only one element if a unique such
     * type exists, otherwise it will contain multiple mutually incompatible supertypes of the object types
     * at which this path ends. The latter case can only occur when the field is a reference field, and there
     * are multiple Java model classes compatible with the field's type.
     *
     * @return the type of the field at which this reference path ends, or null if this reference path does
     *  not have a target field
     */
    public Set<TypeToken<?>> getTargetFieldTypes() {
        return this.targetFieldTypes;
    }

    /**
     * Get the storage ID associated with the target field, if any, in the {@linkplain #getTargetType target object type}.
     *
     * <p>
     * This is just the storage ID of the last field in the path.
     *
     * @return the storage ID of the field at which this reference path ends, or zero if this reference path does
     *  not have a target field
     */
    public int getTargetField() {
        return this.targetFieldStorageId;
    }

    /**
     * Get the storage ID associated with the complex field containing the {@linkplain #getTargetField target field},
     * if any, in the case that the target field is a sub-field of a complex field.
     *
     * @return target field's complex super-field storage ID, or zero if the target field is not a complex sub-field
     * or this reference path does not have a target field
     */
    public int getTargetSuperField() {
        return this.targetSuperFieldStorageId;
    }

    /**
     * Get the storage IDs of the reference fields in this path.
     *
     * <p>
     * Storage ID's will be negated to indicate reference fields traversed in the reverse direction.
     *
     * <p>
     * The path may be empty, i.e., zero references are traversed in the path.
     *
     * <p>
     * Otherwise, the first field is a field in the {@linkplain #getStartType starting object type} and
     * the last field is field in some object type that refers to the {@linkplain #getTargetType target object type}.
     *
     * @return zero or more possibly negated reference field storage IDs
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

    /**
     * Identify the named field at the start of the given field name list in the given {@link JClass}.
     *
     * <p>
     * Either one or two field names will be consumed, depending on whether the field is complex.
     *
     * @param jclass starting type
     * @param fieldNames list of field names
     * @param lastIsSubField true if the last field can be a complex sub-field but not a complex field, false for the reverse,
     *  or null for don't care
     * @return resulting {@link JField}
     * @throws IllegalArgumentException if something is bogus
     */
    private JField findField(JClass<?> jclass, ArrayDeque<String> fieldNames, Boolean lastIsSubField) {

        // Sanity check
        Preconditions.checkArgument(jclass != null, "null jclass");
        Preconditions.checkArgument(fieldNames != null, "null fieldNames");
        Preconditions.checkArgument(!fieldNames.isEmpty(), "empty reference path");

        // Logging
        if (this.log.isDebugEnabled()) {
            this.log.debug("RefPath.findField(): jclass=" + jclass
              + " fieldNames=" + fieldNames + " lastIsSubField=" + lastIsSubField);
        }

        // Get field name and containing type
        final String fieldName = fieldNames.removeFirst();
        String description = "field `" + fieldName + "' in " + jclass.getType();

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

        // Find the JField matching 'fieldName' in jclass
        JField matchingField = jclass.jfieldsByName.get(searchName);
        if (matchingField == null || (explicitStorageId != 0 && matchingField.storageId != explicitStorageId)) {
            throw new IllegalArgumentException("there is no field named `" + searchName + "'"
              + (explicitStorageId != 0 ? " with storage ID " + explicitStorageId : "") + " in " + jclass.getType());
        }

        // Logging
        if (this.log.isDebugEnabled())
            this.log.debug("RefPath.findField(): found field " + matchingField + " in " + jclass.getType());

        // Handle complex fields
        JComplexField superField = null;
        if (matchingField instanceof JComplexField) {
            superField = (JComplexField)matchingField;

            // Logging
            if (this.log.isDebugEnabled())
                this.log.debug("RefPath.findField(): field is a complex field");

            // Last field?
            if (fieldNames.isEmpty()) {

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
                if (this.log.isDebugEnabled())
                    this.log.debug("RefPath.findField(): ended on complex field; result=" + matchingField);
                return matchingField;
            }

            // Get the specified sub-field
            final String subFieldName = fieldNames.removeFirst();
            description = "sub-field `" + subFieldName + "' of complex " + description;
            try {
                matchingField = superField.getSubField(subFieldName);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("invalid " + description + ": " + e.getMessage(), e);
            }

            // Logging
            if (this.log.isDebugEnabled()) {
                this.log.debug("RefPath.findField(): also stepping through sub-field ["
                  + searchName + "." + subFieldName + "] to reach " + matchingField);
            }
        } else if (this.log.isDebugEnabled())
            this.log.debug("RefPath.findField(): field is a simple field");

        // Verify it's OK to end on a complex sub-field (if that's what happened)
        if (fieldNames.isEmpty() && superField != null && Boolean.FALSE.equals(lastIsSubField)) {
            throw new IllegalArgumentException("path may not end on " + description
              + "; instead, specify the complex field itself");
        }

        // Done
        if (this.log.isDebugEnabled())
            this.log.debug("RefPath.findField(): result=" + matchingField);
        return matchingField;
    }

// Cursor

    /**
     * A cursor position in a partially parsed reference path.
     *
     * <p>
     * An instance points to a {@link JClass}, and optionally to a {@link JField} within that class (or within
     * some referring class in the case of a reverse step).
     * It has a list of references it has already traversed, and a list of field names it has yet to traverse.
     * Traversing a field involves two steps: (a) finding the field via {@link #stepThroughField},
     * and (if not done yet) (b) dereferencing that (presumably reference) field via {@link #stepThroughReference}.
     * Note prior to step (a), {@code this.jfield} is null; prior to step (b), {@code this.jfield} is the field
     * we just dereferenced in the previous {@link JClass}.
     *
     * <p>
     * Instances are immutable.
     */
    final class Cursor {

        private final Logger log = LoggerFactory.getLogger(this.getClass());
        private final ArrayList<Integer> referenceFields = new ArrayList<>();
        private final JClass<?> jclass;
        private final JField jfield;
        private final ArrayDeque<String> fieldNames;
        private final boolean reverseStep;

        private Cursor(JClass<?> jclass, ArrayDeque<String> fieldNames) {
            this(new ArrayList<>(0), jclass, null, fieldNames, false);
        }

        private Cursor(ArrayList<Integer> referenceFields, JClass<?> jclass,
          JField jfield, ArrayDeque<String> fieldNames, boolean reverseStep) {
            this.referenceFields.addAll(referenceFields);
            this.jclass = jclass;
            this.jfield = jfield;
            this.fieldNames = fieldNames.clone();
            this.reverseStep = reverseStep;
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

        public boolean isReverseStep() {
            return this.reverseStep;
        }

        /**
         * Step through the next field name and return the resulting cursor.
         *
         * @param lastIsSubField true if the last field can be a complex sub-field but not a complex field, false for the reverse,
         *  or null for don't care
         * @return resulting cursor
         * @throws IllegalArgumentException if step is bogus
         */
        public Set<Cursor> stepThroughField(JSimpleDB jdb, boolean withTargetField, Boolean lastIsSubField) {

            // Sanity check
            Preconditions.checkArgument(!this.fieldNames.isEmpty(), "empty reference path");
            Preconditions.checkState(this.jfield == null, "already stepped through field");

            // Get next field name and containing type
            final ArrayDeque<String> remainingFieldNames = this.fieldNames.clone();
            final String step = remainingFieldNames.peekFirst();

            // Handle forward vs. reverse
            final HashSet<Cursor> newCursors = new HashSet<>(3);
            final Matcher reverseMatcher = Pattern.compile(REV_STEP).matcher(step);
            if (reverseMatcher.matches()) {

                // This field cannot be the target field
                if (withTargetField && remainingFieldNames.isEmpty()) {
                    throw new IllegalArgumentException("Invalid reference path: missing target field after last step `"
                      + step + "'");
                }

                // Get type and field names
                final String typeName = reverseMatcher.group(2);
                final String fieldName = reverseMatcher.group(3);

                // Consume step
                remainingFieldNames.removeFirst();
                if (this.log.isDebugEnabled()) {
                    this.log.debug("RefPath.stepThroughField(): reverse step `" + step
                      + "' -> type `" + typeName + "' field `" + fieldName + "'");
                }

                // Resolve type into all assignable JClass's
                final Class<?> type;
                final SchemaObjectType schemaType = jdb.getNameIndex().getSchemaObjectType(typeName);
                if (schemaType != null)
                    type = jdb.getJClass(schemaType.getStorageId()).getType();
                else {
                    try {
                        type = Class.forName(typeName, false, Thread.currentThread().getContextClassLoader());
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Unknown type `" + typeName
                          + "' in reference path reverse traversal step `" + step + "'");
                    }
                }
                final List<? extends JClass<?>> jclasses = jdb.getJClasses(type);

                // Any types found?
                if (jclasses.isEmpty()) {
                    throw new IllegalArgumentException("Invalid type `" + typeName
                      + "' in reference path reverse traversal step `" + step
                      + "': no schema model types are assignable to `" + typeName + "'");
                }

                // Find field in each type and create corresponding cursors
                for (JClass<?> nextJClass : jclasses) {
                    final ArrayDeque<String> stepFieldNames = new ArrayDeque<>(Arrays.asList(fieldName.split("\\.")));
                    final JField nextJField;
                    try {
                        nextJField = ReferencePath.this.findField(nextJClass, stepFieldNames, true);
                    } catch (IllegalArgumentException e) {
                        continue;
                    }
                    newCursors.add(new Cursor(this.referenceFields, nextJClass, nextJField, remainingFieldNames, true));
                }

                // Any fields found?
                if (newCursors.isEmpty()) {
                    throw new IllegalArgumentException("Invalid reference path reverse traversal step `" + step
                      + "': field `" + fieldName + "' does not exist in "
                      + (schemaType == null ? "any model type assignable to " : "") + "`" + typeName + "'");
                }
            } else {
                assert Pattern.compile(FWD_STEP).matcher(step).matches() : "`" + step + "' is not a forward step";

                // Resolve field
                final JField nextJField = ReferencePath.this.findField(this.jclass, remainingFieldNames, lastIsSubField);
                newCursors.add(new Cursor(this.referenceFields, this.jclass, nextJField, remainingFieldNames, false));
            }
            assert !newCursors.isEmpty();

            // Done
            if (this.log.isDebugEnabled())
                this.log.debug("RefPath.stepThroughField(): result=" + newCursors);
            return newCursors;
        }

        /**
         * Step through the current reference field to the referred-to types.
         */
        public Set<Cursor> stepThroughReference(JSimpleDB jdb) {

            // Logging
            if (this.log.isDebugEnabled())
                this.log.debug("RefPath.stepThroughReference(): this=" + this);

            // Sanity check
            Preconditions.checkState(this.jfield != null, "have not yet stepped through field");
            assert this.jfield != null;
            assert this.reverseStep
              || this.jfield.parent == this.jclass
              || (this.jfield instanceof JSimpleField && ((JSimpleField)this.jfield).getParentField().parent == this.jclass);
            Preconditions.checkArgument(this.jfield instanceof JReferenceField, this.jfield + " is not a reference field");

            // Append reference field to list
            final int stepStorageId = this.reverseStep ? -this.jfield.storageId : this.jfield.storageId;
            final ArrayList<Integer> newReferenceFields = new ArrayList<>(this.referenceFields.size() + 1);
            newReferenceFields.addAll(this.referenceFields);
            newReferenceFields.add(stepStorageId);

            // Advance through the reference, either forward or inverse
            final Class<?> targetType = this.reverseStep ?
              this.jfield.getJClass().type : ((JReferenceField)this.jfield).typeToken.getRawType();
            if (this.log.isDebugEnabled()) {
                this.log.debug("RefPath.stepThroughReference(): targetType="
                  + targetType + " -> " + jdb.getJClasses(targetType));
            }
            final HashSet<Cursor> newCursors = new HashSet<>();
            for (JClass<?> targetJClass : jdb.getJClasses(targetType))
                newCursors.add(new Cursor(newReferenceFields, targetJClass, null, this.fieldNames, false));

            // Done
            if (this.log.isDebugEnabled())
                this.log.debug("RefPath.stepThroughReference(): result=" + newCursors);
            return newCursors;
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
              && Objects.equals(this.jfield, that.jfield)
              && this.reverseStep == that.reverseStep;
        }

        @Override
        public int hashCode() {
            return this.referenceFields.hashCode()
              ^ this.jclass.hashCode()
              ^ Objects.hashCode(this.jfield)
              ^ (this.reverseStep ? 1 : 0);
        }

        @Override
        public String toString() {
            return "Cursor"
              + "[jclass=" + this.jclass
              + (this.jfield != null ? ",jfield=" + this.jfield : "")
              + (!this.fieldNames.isEmpty() ? ",fieldNames=" + this.fieldNames : "")
              + (!this.referenceFields.isEmpty() ? ",refs=" + this.referenceFields : "")
              + (this.reverseStep ? ",reverseStep" : "")
              + "]";
        }
    }
}

