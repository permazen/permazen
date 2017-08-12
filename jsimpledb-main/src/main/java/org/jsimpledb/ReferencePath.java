
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

import org.jsimpledb.core.ObjId;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
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
 * Note that the number of target objects can be vastly different than the number of starting objects, depending on
 * the fan-in/fan-out of the references traversed.
 *
 * <p><b>Specifying Fields</b>
 *
 * <p>
 * The starting object type is always implicit from the context in which the path is used. The {@link String} path describes
 * the reference fields, i.e., the "steps" in the path that travel from the starting object type to the target object type.
 * The steps are separated by dot ({@code "."}) characters.
 *
 * <p>
 * The reference fields in the path may be traversed in either the <i>forward</i> or <i>inverse</i> direction:
 * to traverse a field in the forward direction, specify name of the field; to traverse a field in the inverse direction,
 * specify the name of the field and its containing type using the syntax {@code ^Type:field^}.
 *
 * <p>
 * For complex fields, specify both the field and sub-field: for {@link Set} and {@link java.util.List} fields, the sub-field is
 * always {@code "element"}, while for {@link java.util.Map} fields the sub-field is either {@code "key"} or {@code "value"}.
 * For example, to traverse a map field's {@code key} sub-field, specify {@code "mymap.key"}.
 *
 * <p><b>Target Fields</b>
 *
 * <p>
 * A reference path may optionally have a <b>target field</b> appended to the end of the path. The target field does not have
 * to be a reference field. If a target field is specified, the target object types
 * are restricted to those types containing the target field.
 *
 * <p>
 * Note that to avoid ambiguity it must be known at the time the path is parsed whether the path contains a target field: for
 * example, consider the path {@code "parent.parent"} in the context of a {@code Child} object: if there is a target field,
 * the target object is the child's parent and the target field is the {@code "parent"} field of the child's parent
 * (which just happens to also be a reference field), but if there is no target field, the path simply refers to the child's
 * grandparent.
 *
 * <p><b>Examples</b>
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
 *  <td>The {@code Employee}'s or {@code Asset}'s name</td>
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
 *  <td>{@code "^Employee:assets.element^}<br>&nbsp;&nbsp;{@code .manager.^Employee:manager^.asset.assetId"}</td>
 *  <td>Yes</td>
 *  <td>ID's of all {@code Asset}s owned by direct reports of the manager of the {@code Employee} owning the original
 *      {@code Asset}</td>
 * </tr>
 * </table>
 * </div>
 *
 * <p><b>Fields of Sub-Types</b>
 *
 * <p>
 * The same field can appear in multiple types, e.g., {@code "name"} in the example above appears in both {@code Employee}
 * and {@code Asset}. The set of all possible object types is recalculated at each step in the reference path, including
 * at the last step, which gives the target object type(s). At each intermediate step, as long as the Java types do not
 * contain incompatible definitions for the named field, the step is valid.
 *
 * <p>
 * In rare cases where multiple sub-types of a common super-type type have fields with the same name but different storage IDs,
 * the storage ID may be explicitly specified as a suffix, for example, {@code "name#123"}.
 *
 * <p><b>Using Reference Paths</b>
 *
 * <p>
 * Reference paths may be explicitly created via {@link JSimpleDB#parseReferencePath JSimpleDB.parseReferencePath()}
 * and traversed in the forward direction via {@link JTransaction#followReferencePath JTransaction.followReferencePath()}
 * or in the inverse direction via {@link JTransaction#invertReferencePath JTransaction.invertReferencePath()}.
 *
 * <p>
 * Reference paths are also used implicitly by {@link org.jsimpledb.annotation.OnChange &#64;OnChange} annotations to
 * specify non-local objects for change monitoring.
 *
 * @see JSimpleDB#parseReferencePath JSimpleDB.parseReferencePath()
 * @see JTransaction#followReferencePath JTransaction.followReferencePath()
 * @see JTransaction#invertReferencePath JTransaction.invertReferencePath()
 * @see org.jsimpledb.annotation.OnChange &#64;OnChange
 */
public class ReferencePath {

    private static final String IDENT = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";
    private static final String IDENT_ID = IDENT + "(?:#[0-9]+)?";
    private static final String IDENT_ID_1OR2 = IDENT_ID + "(?:\\." + IDENT_ID + ")?";
    private static final String IDENTS = IDENT + "(?:\\." + IDENT + ")*";
    private static final String FWD_STEP = "(" + IDENT_ID + ")";
    private static final String REV_STEP = "\\^((" + IDENTS + "):(" + IDENT_ID_1OR2 + "))\\^";

    final JSimpleDB jdb;
    final Class<?> startType;
    final ArrayList<Set<Class<?>>> pathTypes;
    final Set<TypeToken<?>> targetFieldTypes;
    final int targetFieldStorageId;
    final int targetSuperFieldStorageId;
    final int[] referenceFieldStorageIds;
    final Set<Cursor> cursors;
    final String path;

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private volatile KeyRanges[] pathKeyRanges;

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
     * @throws IllegalArgumentException if {@code startType} is not compatible with any Java model types
     * @throws IllegalArgumentException if {@code path} is invalid
     */
    ReferencePath(JSimpleDB jdb, Class<?> startType, String path, boolean withTargetField, Boolean lastIsSubField) {

        // Sanity check
        Preconditions.checkArgument(jdb != null, "null jdb");
        Preconditions.checkArgument(startType != null, "null startType");
        Preconditions.checkArgument(path != null, "null path");
        final String errorPrefix = "invalid path `" + path + "': ";
        this.jdb = jdb;
        this.startType = startType;
        this.path = path;

        // Debug
        if (this.log.isTraceEnabled()) {
            this.log.trace("RefPath: START startType=" + startType.getName() + " path=\"" + path
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
        if (this.log.isTraceEnabled())
            this.log.trace("RefPath: fieldNames=" + fieldNames);

        // Get starting types
        final List<? extends JClass<?>> startJClasses = this.jdb.getJClasses(this.startType);
        if (startJClasses.isEmpty()) {
            throw new IllegalArgumentException(errorPrefix
              + "no model type is an instance of path start type " + this.startType.getName());
        }

        // Initialize cursors
        final HashSet<Cursor> remainingCursors = new HashSet<>();
        startJClasses.stream()
          .map(jclass -> new Cursor(jclass, jclass.getType(), fieldNames))
          .forEach(remainingCursors::add);
        if (this.startType.isAssignableFrom(UntypedJObject.class))
          remainingCursors.add(new Cursor(null, UntypedJObject.class, fieldNames));

        // Recursively advance cursors
        IllegalArgumentException error = null;
        final HashSet<Cursor> completedCursors = new HashSet<>();
        while (!remainingCursors.isEmpty()) {

            // Debug
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath: remainingCursors=" + remainingCursors);

            // Advance outstanding cursors
            final HashSet<Cursor> previouslyRemainingCursors = new HashSet<>(remainingCursors);
            remainingCursors.clear();
            for (Cursor cursor : previouslyRemainingCursors) {

                // Debug
                if (this.log.isTraceEnabled())
                    this.log.trace("RefPath: processing remainingCursor " + cursor);

                // Cursors that have no remaining fields, and for which no target field is required, are completed
                if (!withTargetField && !cursor.hasMoreFieldNames()) {
                    if (this.log.isTraceEnabled())
                        this.log.trace("RefPath: remainingCursor " + cursor + " is completed");
                    completedCursors.add(cursor);
                    continue;
                }

                // Try to identify the next field in the path
                final Set<Cursor> newCursors;
                try {
                    newCursors = cursor.identifyNextField(withTargetField, lastIsSubField);
                } catch (IllegalArgumentException e) {
                    if (this.log.isTraceEnabled())
                        this.log.trace("RefPath: identifyNextField() on " + cursor + " failed: " + e.getMessage());
                    error = e;
                    continue;
                }
                if (this.log.isTraceEnabled())
                    this.log.trace("RefPath: identifyNextField() on " + cursor + " succeeded: newCursors=" + newCursors);

                // Debug
                if (this.log.isTraceEnabled())
                    this.log.trace("RefPath: after identifyNextField(), newCursors=" + newCursors);

                // Remove cursors for which we have reached the end of the path
                for (Iterator<Cursor> i = newCursors.iterator(); i.hasNext(); ) {
                    final Cursor newCursor = i.next();
                    if (withTargetField && !newCursor.hasMoreFieldNames()) {
                        if (newCursor.isReverseStep())                              // target field cannot be a reverse step
                            throw new IllegalArgumentException("Invalid reference path: missing target field");
                        if (this.log.isTraceEnabled())
                            this.log.trace("RefPath: newCursor " + cursor + " is completed");
                        completedCursors.add(newCursor);
                        i.remove();
                    }
                }

                // Debug
                if (this.log.isTraceEnabled())
                    this.log.trace("RefPath: after identifyNextField(), remaining newCursors=" + newCursors);

                // Advance the unfinished cursors through the next reference field
                for (Cursor newCursor : newCursors) {

                    // Debug
                    if (this.log.isTraceEnabled())
                        this.log.trace("RefPath: invoking stepThroughReference() on " + newCursor);

                    // "Dereference" the reference field, possibly branching to create multiple new cursors
                    final Set<Cursor> dereferencedCursors;
                    try {
                        dereferencedCursors = newCursor.stepThroughReference();
                    } catch (IllegalArgumentException e) {
                        error = e;
                        continue;
                    }

                    // Debug
                    if (this.log.isTraceEnabled())
                        this.log.trace("RefPath: stepThroughReference() returned " + dereferencedCursors);
                    remainingCursors.addAll(dereferencedCursors);
                }
            }
        }

        // Check for error
        if (this.log.isTraceEnabled())
            this.log.trace("RefPath: remainingCursors=" + remainingCursors + " completedCursors=" + completedCursors);
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

        // Gather and minimize path types
        this.pathTypes = new ArrayList<>(referenceFieldList.size() + 1);
        for (int i = 0; i <= referenceFieldList.size(); i++) {
            final HashSet<Class<?>> types = new HashSet<>();
            for (Cursor cursor : completedCursors) {
                final Class<?> type = cursor.getPathTypes().get(i);
                types.add(type);
                if (this.log.isTraceEnabled())
                    this.log.trace("RefPath: added " + type + " to pathTypes[" + i + "] from " + cursor);
            }
            this.pathTypes.add(ReferencePath.minimizeAndSeal(types));
        }
        assert this.pathTypes.size() == referenceFieldList.size() + 1;

        // Done
        this.referenceFieldStorageIds = Ints.toArray(referenceFieldList);
        this.cursors = completedCursors;

        // Logging
        if (this.log.isTraceEnabled()) {
            this.log.trace("RefPath: DONE: targetFieldStorageId=" + this.targetFieldStorageId
              + " targetSuperFieldStorageId=" + this.targetSuperFieldStorageId + " targetFieldTypes=" + this.targetFieldTypes
              + " references=" + referenceFieldList + " cursors=" + this.cursors + " pathTypes=" + this.pathTypes);
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
        return Util.findLowestCommonAncestorOfClasses(this.getTargetTypes()).getRawType();
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
        return this.getPathTypes().get(this.pathTypes.size() - 1);
    }

    /**
     * Get the set of possible model object types at each step in the path.
     *
     * <p>
     * The returned list always has length one more than the length of the array returned by {@link #getReferenceFields},
     * such that the set at index <i>i</i> contains all possible types found after the <i>i<sup>th</sup></i> step.
     * The first element contains type(s) that are all assignable to the {@linkplain #getStartType starting type}
     * (possibly only the starting type if it was already maximally narrow), and the last element contains the
     * {@linkplain #getTargetTypes target type(s)}.
     *
     * <p>
     * Each set in the returned list will be maximally narrow: it will contain only one element if a unique such type exists,
     * otherwise it will contain multiple mutually incompatible supertypes of the object types at that step.
     *
     * @return list of possible {@link JClass} corresponding to each step
     */
    public List<Set<Class<?>>> getPathTypes() {
        return Collections.unmodifiableList(this.pathTypes);
    }

    KeyRanges[] getPathKeyRanges() {
        if (this.pathKeyRanges == null) {
            final int numJClasses = this.jdb.jclasses.size();
            final KeyRanges[] array = new KeyRanges[this.pathTypes.size()];
            for (int i = 0; i < this.pathTypes.size(); i++) {
                final HashSet<JClass<?>> jclasses = new HashSet<>();
                final Set<Class<?>> types = this.pathTypes.get(i);
                for (Class<?> type : types)
                    jclasses.addAll(this.jdb.getJClasses(type));
                if (jclasses.size() == numJClasses && this.isAnyAssignableFrom(types, UntypedJObject.class))
                    continue;                                                           // no filter needed
                final ArrayList<KeyRange> ranges = new ArrayList<>(jclasses.size());
                for (JClass<?> jclass : jclasses)
                    ranges.add(ObjId.getKeyRange(jclass.storageId));
                array[i] = new KeyRanges(ranges);
            }
            this.pathKeyRanges = array;
        }
        return this.pathKeyRanges;
    }

    private boolean isAnyAssignableFrom(Iterable<? extends Class<?>> tos, Class<?> from) {
        for (Class<?> to : tos) {
            if (to.isAssignableFrom(from))
                return true;
        }
        return false;
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
        if (this.log.isTraceEnabled()) {
            this.log.trace("RefPath.findField(): jclass=" + jclass
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
        if (this.log.isTraceEnabled())
            this.log.trace("RefPath.findField(): found field " + matchingField + " in " + jclass.getType());

        // Handle complex fields
        JComplexField superField = null;
        if (matchingField instanceof JComplexField) {
            superField = (JComplexField)matchingField;

            // Logging
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath.findField(): field is a complex field");

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
                if (this.log.isTraceEnabled())
                    this.log.trace("RefPath.findField(): ended on complex field; result=" + matchingField);
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
            if (this.log.isTraceEnabled()) {
                this.log.trace("RefPath.findField(): also stepping through sub-field ["
                  + searchName + "." + subFieldName + "] to reach " + matchingField);
            }
        } else if (this.log.isTraceEnabled()) {
            if (matchingField instanceof JSimpleField) {
                final JSimpleField simpleField = (JSimpleField)matchingField;
                this.log.trace("RefPath.findField(): field is a simple field of type " + simpleField.getTypeToken());
            } else
                this.log.trace("RefPath.findField(): field is " + matchingField);
        }

        // Verify it's OK to end on a complex sub-field (if that's what happened)
        if (fieldNames.isEmpty() && superField != null && Boolean.FALSE.equals(lastIsSubField)) {
            throw new IllegalArgumentException("path may not end on " + description
              + "; instead, specify the complex field itself");
        }

        // Done
        if (this.log.isTraceEnabled())
            this.log.trace("RefPath.findField(): result=" + matchingField);
        return matchingField;
    }

// Utility methods

    private static Set<Class<?>> minimizeAndSeal(Set<Class<?>> types) {
        final HashSet<Class<?>> minimalTypes = new HashSet<>(types.size());
        for (TypeToken<?> typeToken : Util.findLowestCommonAncestorsOfClasses(types))
            minimalTypes.add((Class<?>)typeToken.getRawType());
        return Collections.unmodifiableSet(minimalTypes);
    }

    private static ArrayList<Class<?>> arrayListOf(Class<?> type) {
        final ArrayList<Class<?>> list = new ArrayList<>(1);
        list.add(type);
        return list;
    }

    private static <T> ArrayList<T> copyAndAppend(List<T> original, T elem) {
        final ArrayList<T> list = new ArrayList<>(original.size() + 1);
        list.addAll(original);
        list.add(elem);
        return list;
    }

// Cursor

    /**
     * A cursor position in a partially parsed reference path.
     *
     * <p>
     * An instance points to a {@link JClass}, and optionally to a {@link JField} within that class (or within
     * some referring class in the case of a reverse step).
     * It has a list of references it has already traversed, and a list of field names it has yet to traverse.
     * Traversing a field involves two steps: (a) finding the field via {@link #identifyNextField},
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
        private final ArrayList<Class<?>> pathTypes = new ArrayList<>();
        private final JClass<?> jclass;                                         // null means UntypedJObject
        private final JField jfield;
        private final ArrayDeque<String> fieldNames;
        private final boolean reverseStep;

        private Cursor(JClass<?> jclass, Class<?> startType, ArrayDeque<String> fieldNames) {
            this(new ArrayList<>(0), ReferencePath.arrayListOf(startType), jclass, null, fieldNames, false);
        }

        private Cursor(ArrayList<Integer> referenceFields, ArrayList<Class<?>> pathTypes, JClass<?> jclass,
          JField jfield, ArrayDeque<String> fieldNames, boolean reverseStep) {
            assert pathTypes.size() == referenceFields.size() + 1;
            this.referenceFields.addAll(referenceFields);
            this.pathTypes.addAll(pathTypes);
            this.jclass = jclass;
            this.jfield = jfield;
            this.fieldNames = fieldNames.clone();
            this.reverseStep = reverseStep;
        }

        public ArrayList<Integer> getReferenceFields() {
            return this.referenceFields;
        }

        public ArrayList<Class<?>> getPathTypes() {
            return this.pathTypes;
        }

        public int getNumRefs() {
            return this.referenceFields.size();
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
         * Get the type associated with the next reference field.
         *
         * @return referred-to type, or null if field is not a reference field
         * @throws IllegalStateException if field not identified yet
         */
        public Class<?> getFieldTargetType() {
            Preconditions.checkState(this.jfield != null, "have not yet stepped through field");
            return this.reverseStep ? this.jfield.getJClass().type :
              this.jfield instanceof JReferenceField ? ((JReferenceField)this.jfield).typeToken.getRawType() : null;
        }

        /**
         * Step through the next field name and return the resulting cursor(s).
         *
         * @param lastIsSubField true if the last field can be a complex sub-field but not a complex field, false for the reverse,
         *  or null for don't care
         * @return resulting cursor(s)
         * @throws IllegalArgumentException if step is bogus
         */
        public Set<Cursor> identifyNextField(boolean withTargetField, Boolean lastIsSubField) {

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
                if (this.log.isTraceEnabled()) {
                    this.log.trace("RefPath.identifyNextField(): reverse step `" + step
                      + "' -> type `" + typeName + "' field `" + fieldName + "'");
                }

                // Resolve type into all assignable JClass's, plus null if untyped objects are possible
                final Class<?> type;
                final SchemaObjectType schemaType = ReferencePath.this.jdb.getNameIndex().getSchemaObjectType(typeName);
                if (schemaType != null)
                    type = ReferencePath.this.jdb.getJClass(schemaType.getStorageId()).getType();
                else {
                    try {
                        type = Class.forName(typeName, false, Thread.currentThread().getContextClassLoader());
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Unknown type `" + typeName
                          + "' in reference path reverse traversal step `" + step + "'");
                    }
                }
                List<? extends JClass<?>> jclasses = ReferencePath.this.jdb.getJClasses(type);
                if (type.isAssignableFrom(UntypedJObject.class)) {
                    final ArrayList<JClass<?>> jclasses2 = new ArrayList<>(jclasses.size() + 1);
                    jclasses2.addAll(jclasses);
                    jclasses2.add(null);
                    jclasses = jclasses2;
                }

                // Any types found?
                if (jclasses.isEmpty()) {
                    throw new IllegalArgumentException("Invalid type `" + typeName
                      + "' in reference path reverse traversal step `" + step
                      + "': no schema model types are assignable to `" + typeName + "'");
                }

                // Find field in each type and create corresponding cursors
                for (JClass<?> nextJClass : jclasses) {
                    if (nextJClass == null)
                        continue;
                    final ArrayDeque<String> stepFieldNames = new ArrayDeque<>(Arrays.asList(fieldName.split("\\.")));
                    final JField nextJField;
                    try {
                        nextJField = ReferencePath.this.findField(nextJClass, stepFieldNames, true);
                    } catch (IllegalArgumentException e) {
                        continue;
                    }
                    newCursors.add(new Cursor(this.referenceFields,
                      this.pathTypes, nextJClass, nextJField, remainingFieldNames, true));
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
                if (this.jclass != null) {
                    final JField nextJField = ReferencePath.this.findField(this.jclass, remainingFieldNames, lastIsSubField);
                    newCursors.add(new Cursor(this.referenceFields,
                      this.pathTypes, this.jclass, nextJField, remainingFieldNames, false));
                }
            }

            // Done
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath.identifyNextField(): result=" + newCursors);
            return newCursors;
        }

        /**
         * Step through the current reference field to the referred-to types.
         */
        public Set<Cursor> stepThroughReference() {

            // Logging
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath.stepThroughReference(): this=" + this);

            // Sanity check
            Preconditions.checkState(this.jfield != null, "have not yet stepped through field");
            assert this.jfield != null;
            assert this.reverseStep
              || this.jfield.parent == this.jclass
              || (this.jfield instanceof JSimpleField && ((JSimpleField)this.jfield).getParentField().parent == this.jclass);
            Preconditions.checkArgument(this.jfield instanceof JReferenceField, this.jfield + " is not a reference field");

            // Append reference field to list
            final int stepStorageId = this.reverseStep ? -this.jfield.storageId : this.jfield.storageId;
            final ArrayList<Integer> newReferenceFields = ReferencePath.copyAndAppend(this.referenceFields, stepStorageId);

            // Advance through the reference, either forward or inverse
            final Class<?> targetType = this.reverseStep ?
              this.jfield.getJClass().type : ((JReferenceField)this.jfield).typeToken.getRawType();
            if (this.log.isTraceEnabled()) {
                this.log.trace("RefPath.stepThroughReference(): targetType="
                  + targetType + " -> " + ReferencePath.this.jdb.getJClasses(targetType));
            }
            final HashSet<Cursor> newCursors = new HashSet<>();
            for (JClass<?> targetJClass : ReferencePath.this.jdb.getJClasses(targetType)) {
                final ArrayList<Class<?>> newPathTypes = ReferencePath.copyAndAppend(this.pathTypes, targetJClass.getType());
                newCursors.add(new Cursor(newReferenceFields, newPathTypes, targetJClass, null, this.fieldNames, false));
            }
            if (targetType.isAssignableFrom(UntypedJObject.class)) {
                final ArrayList<Class<?>> newPathTypes = ReferencePath.copyAndAppend(this.pathTypes, UntypedJObject.class);
                newCursors.add(new Cursor(newReferenceFields, newPathTypes, null, null, this.fieldNames, false));
            }

            // Done
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath.stepThroughReference(): result=" + newCursors);
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
              && this.pathTypes.equals(that.pathTypes)
              && Objects.equals(this.jclass, that.jclass)
              && Objects.equals(this.jfield, that.jfield)
              && this.reverseStep == that.reverseStep;
        }

        @Override
        public int hashCode() {
            return this.referenceFields.hashCode()
              ^ this.pathTypes.hashCode()
              ^ Objects.hashCode(this.jclass)
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
              + (!this.pathTypes.isEmpty() ? ",pathTypes=" + this.pathTypes : "")
              + (this.reverseStep ? ",reverseStep" : "")
              + "]";
        }
    }
}

