
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.OnDelete;
import io.permazen.core.ObjId;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.util.TypeTokens;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Permazen reference paths.
 *
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/prism.min.js"></script>
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/components/prism-java.min.js"></script>
 * <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/themes/prism.min.css" rel="stylesheet"/>
 *
 * <p><b>Overview</b>
 *
 * <p>
 * A reference path defines a bi-directional path of object references, starting from some <em>starting object type(s)</em>
 * and ending up at some <em>target object type(s)</em> by hopping from object to object.
 * Because reference fields are always indexed, given a set of starting or target instances Permazen can efficiently
 * compute the set of objects at the other end of the path. This calculation includes automatic elimination
 * of duplicates caused by loops or multiple paths.
 *
 * <p>
 * The reference fields in the path may be simple fields or sub-fields of complex fields (i.e., list element, set element,
 * map key, or map value), and each field may be traversed in either the forward or inverse direction. In short, a
 * {@link ReferencePath} consists of a set of starting object types, a list of reference fields, and a boolean flag
 * for each field that determines the direction the field should be traversed.
 *
 * <p>
 * When stepping forward through a complex field, or backward through any field, the number of reachable objects can increase.
 * In general, the number of target objects can be vastly different than the number of starting objects, depending on the
 * fan-in/fan-out of the reference fields traversed. This should be kept in mind when considering the use of reference paths.
 *
 * A {@link ReferencePath} containing only forward simple reference fields is termed {@linkplain #isSingular singular}.
 *
 * <p><b>Type Pruning</b>
 *
 * <p>
 * At each step in the path, there is a set of possible <em>current object types</em>: at the initial step, this set
 * is just the starting object types, and after the final step, this set becomes the target object types. At each step,
 * some of the current object types may get pruned because they are incompatible with the next field in the path. This
 * happens when the field is only defined in some of the types (for forward steps), or when the field can only refer to
 * some of the types (for inverse steps). In these cases, the search ends for any pruned types and continues for the
 * remaining types. It is an error if, at any step, all types are pruned, as this would imply that no objects could
 * ever be found.
 *
 * <p><b>String Form</b>
 *
 * <p>
 * Reference paths are denoted in {@link String} form as a concatenation of zero or more <em>reference steps</em>:
 * <ul>
 *  <li>Forward reference steps are denoted by either {@code "->fieldName"} or (rarely) {@code "->TypeName.fieldName"}.
 *      The latter form is only needed to disambiguate when two or more of the current object types define incompatible
 *      fields with the same name.
 *  <li>Inverse reference steps are denoted {@code "<-TypeName.fieldName"} where {@code fieldName} is the name
 *      of a reference field defined in {@code TypeName} (or some sub-type(s) therein).
 * </ul>
 *
 * <p>
 * To parse a reference path into a {@link ReferencePath} instance, the path must be interpreted in the context of
 * some starting object types (see {@link Permazen#parseReferencePath Permazen.parseReferencePath()}).
 *
 * <p><b>Type Names</b>
 *
 * <p>
 * The {@code TypeName} is either the name of an object type in the schema (usually the unqualified name of the corresponding
 * Java model class), or else the fully-qualified name of any Java class or interface.
 *
 * <p><b>Field Names</b>
 *
 * <p>
 * For simple reference fields, the {@code fieldName} is just the field name. For reference fields that are sub-fields
 * of complex fields, {@code fieldName} must specify both the parent field and the sub-field:
 * <ul>
 *  <li>For {@code Map} fields, specify the sub-field via either {@code myfield.key} or {@code myfield.value}.
 *  <li>For {@code List} and {@code Set} fields, the only sub-field is {@code element}, so you can specify
 *      {@code myfield.element} or abbreviate as {@code myfield}.
 * </ul>
 *
 * <p><b>Examples</b>
 *
 * <p>
 * Consider the following model classes:
 *
 * <pre><code class="language-java">
 * &#64;PermazenType
 * public interface Animal&lt;T extends Animal&lt;T&gt;&gt; {
 *
 *     T getParent();
 *     void setParent(T parent);
 *
 *     Set&lt;Animal&lt;?&gt;&gt; getEnemies();
 * }
 *
 * &#64;PermazenType
 * public interface Elephant extends Animal&lt;Elephant&gt; {
 *
 *     Elephant getFriend();
 *     void setFriend(Elephant friend);
 * }
 *
 * &#64;PermazenType
 * public interface Giraffe extends Animal&lt;Giraffe&gt; {
 *
 *     Giraffe getFriend();
 *     void setFriend(Giraffe friend);
 * }
 * </code></pre>
 *
 * Then the reference paths below would have the following meanings:
 *
 * <div style="margin-left: 20px;">
 * <table class="striped">
 * <caption>Reference Path Examples</caption>
 * <tr style="bgcolor:#ccffcc">
 *  <th style="font-weight: bold; text-align: left">Start&nbsp;Type</th>
 *  <th style="font-weight: bold; text-align: left">Path</th>
 *  <th style="font-weight: bold; text-align: left">Target Types</th>
 *  <th style="font-weight: bold; text-align: left">Description</th>
 * </tr>
 * <tr>
 *  <td>{@code Elephant}</td>
 *  <td>{@code ""}</td>
 *  <td>{@code Elephant}</td>
 *  <td>The starting {@code Elephant}</td>
 * </tr>
 * <tr>
 *  <td>{@code Elephant}</td>
 *  <td>{@code "->parent"}</td>
 *  <td>{@code Elephant}</td>
 *  <td>The {@code Elephant}'s parent</td>
 * </tr>
 * <tr>
 *  <td>{@code Giraffe}</td>
 *  <td>{@code "->parent"}</td>
 *  <td>{@code Giraffe}</td>
 *  <td>The {@code Giraffe}'s parent</td>
 * </tr>
 * <tr>
 *  <td>{@code Animal}</td>
 *  <td>{@code "->parent"}</td>
 *  <td>{@code Elephant},&nbsp;{@code Giraffe}</td>
 *  <td>The {@code Animals}'s parent</td>
 * </tr>
 * <tr>
 *  <td>{@code Elephant}</td>
 *  <td>{@code "<-Elephant.enemies"}</td>
 *  <td>{@code Elephant}</td>
 *  <td>All {@code Elephant}s for whom the original {@code Elephant} is an enemy</td>
 * </tr>
 * <tr>
 *  <td>{@code Elephant}</td>
 *  <td>{@code "<-Animal.enemies"}</td>
 *  <td>{@code Elephant},&nbsp;{@code Giraffe}</td>
 *  <td>All {@code Animal}s for whom the original {@code Elephant} is an enemy</td>
 * </tr>
 * <tr>
 *  <td>{@code Elephant}</td>
 *  <td>{@code "<-friend"}</td>
 *  <td>{@code Elephant}</td>
 *  <td>All {@code Elephant}s for whom the original {@code Elephant} is their friend</td>
 * </tr>
 * <tr>
 *  <td>{@code Animal}</td>
 *  <td>{@code "<-friend"}</td>
 *  <td>{@code Elephant},&nbsp;{@code Giraffe}</td>
 *  <td>All {@code Animal}s for whom the original {@code Animal} is their friend</td>
 * </tr>
 * <tr>
 *  <td>{@code Elephant}</td>
 *  <td>{@code "->friend<-Giraffe.enemies"}</td>
 *  <td>{@code Giraffe}</td>
 *  <td>All {@code Giraffe}s for whom the original {@code Elephant}'s friend is an enemy</td>
 * </tr>
 * <tr>
 *  <td>{@code Elephant}</td>
 *  <td>{@code "->enemies<-Giraffe.friend}<br>  {@code ->enemies<-Elephant.friend"}</td>
 *  <td>{@code Elephant}</td>
 *  <td>All {@code Elephant}s who's friend is an enemy of some {@code Giraffe} for whom one of the original
 *      {@code Elephant}'s enemies is their friend</td>
 * </tr>
 * <tr>
 *  <td>{@code Elephant}</td>
 *  <td>{@code "<-Giraffe.friend"}</td>
 *  <td>N/A</td>
 *  <td>Invalid - it's not possible for an {@code Elephant} to be a {@code Giraffe}'s friend</td>
 * </tr>
 * </table>
 * </div>
 *
 * <p><b>Using Reference Paths</b>
 *
 * <p>
 * Reference paths may be explicitly created via {@link Permazen#parseReferencePath Permazen.parseReferencePath()}
 * and traversed in the forward direction via
 * {@link PermazenTransaction#followReferencePath PermazenTransaction.followReferencePath()}
 * or in the inverse direction via {@link PermazenTransaction#invertReferencePath PermazenTransaction.invertReferencePath()}.
 *
 * <p>
 * The {@link io.permazen.annotation.ReferencePath &#64;ReferencePath} annotation can be used to auto-generate methods
 * that traverse reference paths.
 *
 * <p>
 * Reference paths are also used by {@link OnChange &#64;OnChange} and {@link OnDelete &#64;OnDelete} annotations to specify
 * non-local objects for monitoring.
 *
 * @see Permazen#parseReferencePath Permazen.parseReferencePath()
 * @see PermazenTransaction#followReferencePath PermazenTransaction.followReferencePath()
 * @see PermazenTransaction#invertReferencePath PermazenTransaction.invertReferencePath()
 * @see io.permazen.annotation.ReferencePath &#64;ReferencePath
 * @see OnChange &#64;OnChange
 * @see OnDelete &#64;OnDelete
 */
public class ReferencePath {

    private static final String FWD_PREFIX = "->";
    private static final String REV_PREFIX = "<-";

    final Permazen pdb;
    final String path;
    final boolean singular;
    final int[] storageIds;
    final ArrayList<Set<PermazenClass<?>>> currentTypesList;

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private volatile KeyRanges[] pathKeyRanges;

    /**
     * Constructor.
     *
     * @param pdb {@link Permazen} against which to resolve object and field names
     * @param startTypes starting model types for the path, with null meaning {@link UntypedPermazenObject}
     * @param path reference path in string form
     * @throws IllegalArgumentException if {@code startTypes} is empty
     * @throws IllegalArgumentException if {@code path} is invalid
     * @throws IllegalArgumentException if any parameter is null
     */
    ReferencePath(Permazen pdb, Collection<PermazenClass<?>> startTypes, String path) {

        // Sanity check
        Preconditions.checkArgument(pdb != null, "null pdb");
        Preconditions.checkArgument(startTypes != null, "null startTypes");
        Preconditions.checkArgument(!startTypes.isEmpty(), "empty startTypes");
        Preconditions.checkArgument(path != null, "null path");
        this.pdb = pdb;
        this.path = path;

        // Debug
        if (this.log.isTraceEnabled())
            this.log.trace("RefPath: START path=\"{}\" startTypes={}", path, debugFormat(startTypes));

        // Split the path into steps, each starting with "->" or "<-"
        final List<String> steps = new ArrayList<>();           // steps without prefixes
        final List<Boolean> inverses = new ArrayList<>();       // true = inverse, false = forward
        final Pattern prefixPattern = Pattern.compile(String.format("(%s|%s)",
          Pattern.quote(FWD_PREFIX), Pattern.quote(REV_PREFIX)));
        final String errorPrefix = "invalid path \"" + path + "\"";
        while (!path.isEmpty()) {
            Matcher matcher = prefixPattern.matcher(path);
            if (!matcher.lookingAt()) {
                throw new IllegalArgumentException(String.format(
                  "%s: steps must start with either \"%s\" or \"%s\"", errorPrefix, FWD_PREFIX, REV_PREFIX));
            }
            final String prefix = matcher.group();
            final boolean inverse = prefix.equals(REV_PREFIX);
            path = path.substring(prefix.length());
            final String step = (matcher = prefixPattern.matcher(path)).find() ? path.substring(0, matcher.start()) : path;
            if (step.isEmpty())
                throw new IllegalArgumentException(String.format("%s: invalid empty step \"%s\"", errorPrefix, prefix));
            steps.add(step);
            inverses.add(inverse);
            path = path.substring(step.length());
        }

        // Debug
        if (this.log.isTraceEnabled())
            this.log.trace("RefPath: steps={}", steps);

        // Initialize cursors
        HashSet<Cursor> cursors = new HashSet<>();
        for (PermazenClass<?> startType : startTypes)
            cursors.add(new Cursor(startType));

        // Advance cursors until all steps are completed or they all peter out
        for (int i = 0; i < steps.size(); i++) {
            final String step = steps.get(i);
            final boolean inverse = inverses.get(i);

            // Debug
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath: STEP {} inverse={} cursors={}", step, inverse, debugFormat(cursors));

            // Advance the remaining cursors
            final HashSet<Cursor> newCursors = new HashSet<>();
            IllegalArgumentException error = null;
            for (Cursor cursor : cursors) {

                // Debug
                if (this.log.isTraceEnabled())
                    this.log.trace("RefPath: processing cursor {}", cursor);

                // Try to advance this cursor
                final Set<Cursor> advancedCursors;
                try {
                    advancedCursors = cursor.advance(inverse, step);
                } catch (IllegalArgumentException e) {
                    if (this.log.isTraceEnabled())
                        this.log.trace("RefPath: advance({}, \"{}\") on {} failed: {}", inverse, step, cursor, e.getMessage());
                    if (error == null || cursor.pclass != null)
                        error = e;
                    continue;
                }
                if (this.log.isTraceEnabled()) {
                    this.log.trace("RefPath: advance({}, \"{}\") on {} succeeded: advancedCursors={}",
                      inverse, step, cursor, debugFormat(advancedCursors));
                }

                // Add new cursors to next cursor set
                newCursors.addAll(advancedCursors);
            }

            // Any cursors remaining?
            if ((cursors = newCursors).isEmpty())
                throw error;
        }
        if (this.log.isTraceEnabled())
            this.log.trace("RefPath: final cursors={}", debugFormat(cursors));

        // Verify all cursors stepped through the same fields
        final Iterator<Cursor> cursortIterator = cursors.iterator();
        this.storageIds = cursortIterator.next().getStorageIds();
        cursortIterator.forEachRemaining(cursor -> {
            if (!Arrays.equals(cursor.getStorageIds(), storageIds)) {
                throw new IllegalArgumentException(String.format(
                  "%s: path is ambiguous due to traversal of different fields in different types", errorPrefix));
            }
        });

        // Record the current types at each step
        this.currentTypesList = new ArrayList<>(steps.size() + 1);
        for (int i = 0; i <= steps.size(); i++) {

            // Combine current types at this step from all cursors
            final int i2 = i;
            final Set<PermazenClass<?>> currentTypes = cursors.stream()
              .map(Cursor::getCurrentTypesList)
              .map(list -> list.get(i2))
              .collect(Collectors.toSet());

            // If UntypedObject is possible, then all model types should be possible
            assert !currentTypes.contains(null) || currentTypes.containsAll(this.pdb.pclasses);

            // Add to current types to list
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath: currentTypesList[{}] = {}", this.currentTypesList.size(), currentTypes);
            this.currentTypesList.add(Collections.unmodifiableSet(currentTypes));
        }

        // Record whether path is singular
        this.singular = cursors.iterator().next().isSingular();     // all cursors should be the same on this question

        // Logging
        if (this.log.isTraceEnabled()) {
            this.log.trace("RefPath: DONE: singular={} fields={} currentTypesList={}",
              singular, Ints.asList(this.storageIds), debugFormat(this.currentTypesList));
        }
    }

    private String debugFormat(Collection<?> items) {
        if (items == null)
            return "null";
        if (items.isEmpty())
            return "EMPTY";
        return "\n  " + items.stream().map(String::valueOf).collect(Collectors.joining("\n  "));
    }

    /**
     * Get the possible model object types for the objects at the start of this path.
     *
     * <p>
     * This method returns the first set in the list returned by {@link #getCurrentTypesList}.
     * If this path has zero length, then this method returns the same set as {@link #getTargetTypes}.
     *
     * <p>
     * The returned set will contain a null element when the target object can possibly be an {@link UntypedPermazenObject}.
     *
     * @return non-empty set of model object types at which this reference path starts, possibly including null
     */
    public Set<PermazenClass<?>> getStartingTypes() {
        return this.currentTypesList.get(0);
    }

    /**
     * Get the narrowest possible Java type of the object(s) at which this path starts.
     *
     * <p>
     * The returned type will be as narrow as possible while still including all possibilities, but note that it's
     * possible for there to be multiple candidates for the "starting type", none of which is a sub-type of any other.
     * To retrieve all such starting types, use {@link #getStartingTypes}; this method just invokes
     * {@link TypeTokens#findLowestCommonAncestorOfClasses TypeTokens.findLowestCommonAncestorOfClasses()} on that result.
     *
     * @return the Java type at which this reference path starts
     */
    public Class<?> getStartingType() {
        return TypeTokens.findLowestCommonAncestorOfClasses(ReferencePath.toClasses(this.getStartingTypes())).getRawType();
    }

    /**
     * Get the possible model object types for the objects at the end of this path.
     *
     * <p>
     * This method returns the last set in the list returned by {@link #getCurrentTypesList}.
     * If this path has zero length, then this method returns the same set as {@link #getStartingTypes}.
     *
     * <p>
     * The returned set will contain a null element when the target object can possibly be an {@link UntypedPermazenObject}.
     *
     * @return non-empty set of model object types at which this reference path ends, possibly including null
     */
    public Set<PermazenClass<?>> getTargetTypes() {
        return this.currentTypesList.get(this.currentTypesList.size() - 1);
    }

    /**
     * Get the narrowest possible Java type of the object(s) at which this path ends.
     *
     * <p>
     * The returned type will be as narrow as possible while still including all possibilities, but note that it's
     * possible for there to be multiple candidates for the "target type", none of which is a sub-type of any other.
     * To retrieve all such target types, use {@link #getTargetTypes}; this method just invokes
     * {@link TypeTokens#findLowestCommonAncestorOfClasses TypeTokens.findLowestCommonAncestorOfClasses()} on that result.
     *
     * @return the Java type at which this reference path ends
     */
    public Class<?> getTargetType() {
        return TypeTokens.findLowestCommonAncestorOfClasses(ReferencePath.toClasses(this.getTargetTypes())).getRawType();
    }

    /**
     * Get the current object types at each step in the path.
     *
     * <p>
     * The returned list always has length one more than the length of the array returned by {@link #getReferenceFields},
     * such that the set at index <i>i</i> contains all possible types found after the <i>i<sup>th</sup></i> step.
     * The first element contains the {@linkplain #getStartingTypes starting object types}
     * and the last element contains the {@linkplain #getTargetTypes target types}.
     *
     * <p>
     * A set in the list will contain a null element if an object at that step can possibly be an {@link UntypedPermazenObject}.
     *
     * @return list of the possible {@link PermazenClass}'s appearing at each step in this path, each of which is
     *  non-empty and may include null
     */
    public List<Set<PermazenClass<?>>> getCurrentTypesList() {
        return Collections.unmodifiableList(this.currentTypesList);
    }

    /**
     * Get the storage IDs of the reference fields in this path in the order they occur.
     *
     * <p>
     * Storage ID's will be negated to indicate reference fields traversed in the inverse direction.
     *
     * <p>
     * The result will be empty if this path is empty.
     *
     * @return zero or more possibly negated reference field storage IDs
     */
    public int[] getReferenceFields() {
        return this.storageIds.clone();
    }

    /**
     * Determine whether traversing this path can result in only one object being found.
     *
     * <p>
     * An empty path is always singular - it always returns just the starting object.
     *
     * @return true if this path only includes forward simple field references, otherwise false
     */
    public boolean isSingular() {
        return this.singular;
    }

    /**
     * Get the number of steps in this reference path.
     *
     * @return the length of this path
     */
    public int size() {
        return this.storageIds.length;
    }

    /**
     * Determine whether this path is empty, i.e., contains zero steps.
     *
     * @return true if this path is empty
     */
    public boolean isEmpty() {
        return this.size() == 0;
    }

// Object

    /**
     * Get the {@link String} form of the path associated with this instance.
     */
    @Override
    public String toString() {
        return this.path;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ReferencePath that = (ReferencePath)obj;
        return this.pdb.equals(that.pdb)
          && this.path.equals(that.path);
          // the other fields are derived from the above
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode()
          ^ this.pdb.hashCode()
          ^ this.path.hashCode();
    }

// Package Methods

    KeyRanges[] getPathKeyRanges() {
        if (this.pathKeyRanges == null) {
            final int numPClasses = this.pdb.pclasses.size();
            final KeyRanges[] array = new KeyRanges[this.currentTypesList.size()];
            for (int i = 0; i < this.currentTypesList.size(); i++) {

                // Get the current types at this step
                final Set<PermazenClass<?>> pclasses = this.currentTypesList.get(i);

                // If every model type plus UntypedPermazenObject is possible, then no filter is needed
                if (pclasses.size() > numPClasses)
                    continue;

                // Restrict to the specific PermazenClass's ranges
                final ArrayList<KeyRange> ranges = new ArrayList<>(pclasses.size());
                for (PermazenClass<?> pclass : pclasses)
                    ranges.add(ObjId.getKeyRange(pclass.storageId));

                // Build filter
                array[i] = new KeyRanges(ranges);
            }
            this.pathKeyRanges = array;
        }
        return this.pathKeyRanges;
    }

// Utility methods

    private static Stream<Class<?>> toClasses(Set<PermazenClass<?>> pclasses) {
        return pclasses.stream()
          .map(pclass -> pclass != null ? pclass.type : UntypedPermazenObject.class);
    }

// Cursor

    /**
     * A cursor position in a partially parsed reference path.
     *
     * <p>
     * Instances are immutable.
     */
    final class Cursor {

        private final Logger log = LoggerFactory.getLogger(this.getClass());
        private final int stepsSoFar;                           // number of previous steps/cursors
        private final Cursor previousCursor;                    // previous step's cursor (null if none)
        private final int previousStorageId;                    // previous step's storage ID (zero if none)
        private final PermazenClass<?> pclass;                  // current step's PermazenClass, or null for UntypedPermazenObject
        private final boolean singular;                         // true if path is singular so far

        // Constructor for initial cursors
        private Cursor(PermazenClass<?> pclass) {
            this(null, 0, pclass, true);
        }

        // Constructor for additional cursors
        private Cursor(Cursor previousCursor, int previousStorageId, PermazenClass<?> pclass, boolean singular) {
            this.stepsSoFar = previousCursor != null ? previousCursor.stepsSoFar + 1 : 0;
            this.previousCursor = previousCursor;
            this.previousStorageId = previousStorageId;
            this.pclass = pclass;
            this.singular = singular;
        }

        // Get the number of previous steps/cursors
        public int getStepsSoFar() {
            return this.stepsSoFar;
        }

        // Get the reference field storage ID's corresponding to all previous steps, negated for inverse steps
        public int[] getStorageIds() {
            return Stream.concat(this.streamPrevious(), Stream.of(this))
              .skip(1)
              .mapToInt(cursor -> cursor.previousStorageId)
              .toArray();
        }

        // Get the current object type for all previous steps plus this next one
        public List<PermazenClass<?>> getCurrentTypesList() {
            return Stream.concat(this.streamPrevious().map(cursor -> cursor.pclass), Stream.of(this.pclass))
              .collect(Collectors.toList());
        }

        // Does this path only return ever a single object?
        public boolean isSingular() {
            return this.singular;
        }

        // Stream the previous cursors in order
        private Stream<Cursor> streamPrevious() {
            final Cursor[] cursors = new Cursor[this.stepsSoFar];
            int index = this.stepsSoFar;
            for (Cursor cursor = this.previousCursor; cursor != null; cursor = cursor.previousCursor)
                cursors[--index] = cursor;
            return Stream.of(cursors);
        }

        /**
         * Advance through the given step and return the resulting new cursors.
         *
         * @param inverse true for inverse step
         * @param step step to advance without prefix
         * @return resulting cursors
         * @throws IllegalArgumentException if step is invalid
         */
        public Set<Cursor> advance(boolean inverse, String step) {

            // Sanity check
            Preconditions.checkArgument(step != null, "null step");
            final String errorPrefix = String.format("invalid %s step \"%s\"", inverse ? "inverse" : "forward", step);

            // Parse next step
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath.advance(): next step: {} \"{}\"", inverse ? "inverse" : "forward", step);

            // Resolve the next step
            FieldResolution nextStep = this.resolveStep(errorPrefix, inverse, step);
            final Set<PermazenClass<?>> nextPClasses = nextStep.types();
            final PermazenReferenceField nextPField = nextStep.field();

            // Get the storage ID representing this step, negated for inverse steps
            final int storageId = inverse ? -nextPField.storageId : nextPField.storageId;

            // Can the new cursor produce multiple objects? Yes unless we've only seen forward simple references so far.
            final boolean nextSingular = this.singular && !inverse && nextPField.getParentField() == null;

            // Debug
            if (this.log.isTraceEnabled()) {
                this.log.trace("RefPath.advance(): nextPField={} storageId={} nextSingular={} nextPClasses={}",
                  nextPField, storageId, nextSingular, debugFormat(nextPClasses));
            }

            // Create new cursor(s)
            final HashSet<Cursor> newCursors = new HashSet<>(3);
            for (PermazenClass<?> nextPClass : nextPClasses)
                newCursors.add(new Cursor(this, storageId, nextPClass, nextSingular));
            assert !newCursors.isEmpty();

            // Done
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath.advance(): result={}", debugFormat(newCursors));
            return newCursors;
        }

        // Resolve the next step. This is complicated because there are lots of possibilities because a dot "." can separate
        // two Java package/class name components, a type name and a field name, or a field name and a sub-field name.
        private FieldResolution resolveStep(String errorPrefix, final boolean inverse, final String step) {

            // Are there any dots? If not, this is easy.
            final int dot1 = step.lastIndexOf('.');         // the very last dot
            if (dot1 == -1) {

                // Inverse steps must be qualified
                if (inverse)
                    throw new IllegalArgumentException(errorPrefix);

                // It must be an unqualified forward step, i.e., simple field name
                return this.resolveUnqualifiedStep(errorPrefix, step);
            }

            // If there is only one dot, this is a forward step, and the step matches an unqualified field name, then use it
            final int dot2 = step.lastIndexOf('.', dot1 - 1);
            if (dot2 == -1 && !inverse) {
                try {
                    return this.resolveUnqualifiedStep(errorPrefix, step);
                } catch (IllegalArgumentException e) {
                    // Nope, so from now on assume forward steps are qualified
                }
            }

            // What is the current type?
            final Class<?> currentType = Optional.ofNullable(this.pclass)
              .<Class<?>>map(PermazenClass::getType)
              .orElse(UntypedPermazenObject.class);

            // For forward steps, the TypeName qualifier must be restricted to the current type
            final Class<?> upperBound = !inverse ? currentType : null;

            // Resolve this qualified step
            FieldResolution nextStep = this.resolveQualifiedStep(errorPrefix, step, upperBound);

            // For inverse steps, verify that the reference field can actually refer to the current type
            if (inverse && !nextStep.field().typeToken.getRawType().isAssignableFrom(currentType)) {
                throw new IllegalArgumentException(String.format(
                  "%s: %s can't refer to %s", errorPrefix, nextStep.field(), currentType));
            }

            // For forward steps, the next set of current types derives from what types the reference field can point to
            if (!inverse)
                nextStep = new FieldResolution(ReferencePath.this.pdb, nextStep.field());

            // Done
            return nextStep;
        }

        // Resolve an unqualified step like "[fieldName]", where "fieldName" is a simple field, or possibly a complex sub-field.
        private FieldResolution resolveUnqualifiedStep(String errorPrefix, final String fieldName) {

            // Handle the UntypedPermazenObject case
            if (this.pclass == null) {
                throw new IllegalArgumentException(String.format(
                  "%s: field \"%s\" not found in %s", errorPrefix, fieldName, UntypedPermazenObject.class));
            }

            // Find the named field
            final PermazenReferenceField field;
            try {
                field = (PermazenReferenceField)Util.findSimpleField(this.pclass, fieldName);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("%s: %s", errorPrefix, e.getMessage()), e);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(String.format(
                  "%s: field \"%s\" in %s is not a reference field", errorPrefix, fieldName, this.pclass));
            }
            if (field == null) {
                throw new IllegalArgumentException(String.format(
                  "%s: field \"%s\" not found in %s", errorPrefix, fieldName, this.pclass));
            }

            // Done
            return new FieldResolution(ReferencePath.this.pdb, field);
        }

        // Resolve a qualified step like "[typeName].[fieldName]", where "typeName" could be either a schema object
        // name or fully qualified Java class name, and "fieldName" is a simple field, or possibly a complex sub-field.
        private FieldResolution resolveQualifiedStep(String errorPrefix, final String step, Class<?> upperBound) {

            // Find the last dot
            final int dot1 = step.lastIndexOf('.');
            if (dot1 == -1) {
                throw new IllegalArgumentException(String.format(
                  "%s: field name must be qualified with a type name", errorPrefix));
            }

            // Parse into TypeName and fieldName, but note the ambiguity: "foo.bar.key" could
            // mean either { type="foo", field="bar.key" } or { type="foo.bar", field="key" }.
            // If field name has three or more components, try interpreting it both ways.
            final int dot2 = step.lastIndexOf('.', dot1 - 1);
            final FieldResolution try1 = this.resolveQualifiedStep(errorPrefix,
              step.substring(0, dot1), step.substring(dot1 + 1), upperBound);
            final FieldResolution try2 = dot2 != -1 ?
              this.resolveQualifiedStep(errorPrefix, step.substring(0, dot2), step.substring(dot2 + 1), upperBound) : null;
            if (this.log.isTraceEnabled())
                this.log.trace("RefPath.advance(): qualified candidates: try1={} try2={} bound={}", try1, try2, upperBound);

            // Anything found?
            if (try1 == null && try2 == null) {
                throw new IllegalArgumentException(String.format(
                  "%s: no such type and/or reference field found%s",
                  errorPrefix, upperBound != null ? String.format(" in the context of %s", upperBound) : ""));
            }

            // Verify two different interpretations are consistent (unlikely!)
            if (try1 != null && try2 != null && !try1.isConsistentWith(try2)) {
                throw new IllegalArgumentException(String.format(
                  "%s: ambiguous reference; matched %s in %s and %s in %s",
                  errorPrefix, try1.field(), try1.types(), try2.field(), try2.types()));
            }

            // Done
            return try1 != null ? try1 : try2;
        }

        // Resolve a qualified step "[typeName].[fieldName]" where "typeName" and "fieldName" are given.
        // The upperBound, if any, applies to the type name.
        private FieldResolution resolveQualifiedStep(String errorPrefix, String typeName, String fieldName, Class<?> upperBound) {

            // Try resolving type name as a schema model object type
            final Class<?> modelType = Optional.of(ReferencePath.this.pdb.pclassesByName)
              .map(map -> map.get(typeName))
              .map(PermazenClass::getType)
              .orElse(null);

            // Try resolving type name as a regular Java class
            Class<?> javaType = null;
            try {
                javaType = Class.forName(typeName, false, ReferencePath.this.pdb.loader.getParent());
            } catch (ClassNotFoundException e) {
                // ignore
            }

            // Anything matched?
            if (modelType == null && javaType == null)
                return null;

            // Check for ambiguity
            if (modelType != null && javaType != null && modelType != javaType) {
                throw new IllegalArgumentException(String.format(
                  "%s: ambiguous type name \"{}\" matches both %s and %s", errorPrefix, typeName, modelType, javaType));
            }

            // Get the type we found
            if (javaType == null)
                javaType = modelType;

            // Find the field, gather matching PermazenClass's, and verify field appears consistently
            PermazenReferenceField field = null;
            final HashSet<PermazenClass<?>> pclasses = new HashSet<>();
            for (PermazenClass<?> nextPClass : ReferencePath.this.pdb.getPermazenClasses(javaType)) {

                // Apply upper bound, if any
                if (upperBound != null && !upperBound.isAssignableFrom(nextPClass.getType()))
                    continue;

                // Find the reference field
                final PermazenReferenceField candidateField;
                try {
                    candidateField = (PermazenReferenceField)Util.findSimpleField(nextPClass, fieldName);
                } catch (IllegalArgumentException | ClassCastException e) {
                    continue;
                }
                if (candidateField == null)
                    continue;

                // Check for ambiguity
                if (field == null)
                    field = candidateField;
                else if (!candidateField.getSchemaId().equals(field.getSchemaId())) {
                    throw new IllegalArgumentException(String.format(
                      "%s: ambiguous field \"%s\" in %s matches both %s and %s",
                      errorPrefix, fieldName, javaType, field, candidateField));
                }

                // Add pclass
                pclasses.add(nextPClass);
            }

            // Return what we found, if anything
            return field != null ? new FieldResolution(pclasses, field) : null;
        }

    // FieldResolution

        private record FieldResolution(Set<PermazenClass<?>> types, PermazenReferenceField field) {

            FieldResolution(Permazen pdb, PermazenReferenceField field) {
                this(new HashSet<>(pdb.getPermazenClasses(field.typeToken.getRawType())), field);
            }

            boolean isConsistentWith(FieldResolution that) {
                return this.types().equals(that.types()) && this.field().getSchemaId().equals(that.field().getSchemaId());
            }
        }

    // Object

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final Cursor that = (Cursor)obj;
            return Objects.equals(this.previousCursor, that.previousCursor)
              && this.previousStorageId == that.previousStorageId
              && Objects.equals(this.pclass, that.pclass)
              && this.singular == that.singular;
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode()
              ^ Objects.hashCode(this.previousCursor)
              ^ Integer.hashCode(this.previousStorageId)
              ^ Objects.hashCode(this.pclass)
              ^ Boolean.hashCode(this.singular);
        }

        @Override
        public String toString() {
            return "Cursor"
              + "[stepsSoFar=" + this.stepsSoFar
              + (this.previousCursor != null ? ",previousStorageId=" + this.previousStorageId : "")
              + ",pclass=" + this.pclass
              + ",singular=" + this.singular
              + "]";
        }
    }
}
