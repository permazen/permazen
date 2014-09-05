
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates JSimpleDB model class methods that access the index associated with an indexed field.
 *
 * <p>
 * The annotated method must be abstract, take zero parameters, and return
 * {@link java.util.NavigableMap NavigableMap}{@code <V, }{@link java.util.NavigableSet NavigableSet}{@code <T>>}, where
 * {@code V} is the type of the indexed field's values, and {@code T} is the type of the Java model object
 * containing the field.
 * </p>
 *
 * <p>
 * For example:
 * <pre>
 * &#64;JSimpleClass(storageId = 10)
 * public abstract class <b>Person</b> {
 *
 *     &#64;JSetField(storageId = 11, element = @JField(storageId = 12, <b>indexed = true</b>))
 *     public abstract Set&lt;<b>String</b>&gt; getNicknames();
 *
 *     <b>&#64;IndexQuery("nicknames")</b>
 *     private abstract NavigableMap&lt;<b>String</b>, NavigableSet&lt;<b>Person</b>&gt;&gt; queryNicknames();
 *
 *     // Get all people with the given nickname
 *     public Set&lt;Person&gt; getPeopleWithNickame(String nickname) {
 *         final Set&lt;Person&gt; people = this.queryNicknames().get(nickname);
 *         return people != null ? people : Collections.&lt;Person&gt;emptySet();
 *     }
 *
 *     // Get all known nicknames, sorted by nickname
 *     public NavigableSet&lt;String&gt; getAllNickames() {
 *         return this.queryNicknames().navigableKeySet();
 *     }
 *
 *     // Do an "inner join" on two nicknames
 *     public NavigableSet&lt;Person&gt; getPeopleWithBothNicknames(String nickname1, String nickname2) {
 *         return NaviableSets.intersection(this.getPeopleWithNickame(nickname1), this.getPeopleWithNickame(nickname2));
 *     }
 * }
 * </pre>
 * </p>
 *
 * <p>
 * The returned {@link java.util.NavigableMap NavigableMap} contains an entry for every value that is actually present
 * in the field in some object, with the corresponding value being the set of those objects. It represents a "live" view:
 * if an indexed field is modified, any previously returned {@link java.util.NavigableMap NavigableMap} updates itself
 * immediately (whether "live" iterators update depends on the behavior of the underlying key/value database; see
 * {@link org.jsimpledb.kv.KVStore#getRange KVStore.getRange()}).
 * </p>
 *
 * <p>
 * For list and map fields, you may also query the index for additional information beyond just which objects contain
 * the queried value in the indexed field: in the case of a list, you can also get the
 * list index, and in the case of a map, you can also get the corresponding key or value. For these kinds of query, specify the
 * type {@code T} to be either {@link org.jsimpledb.ListIndexEntry},
 * {@link org.jsimpledb.MapKeyIndexEntry}, or {@link org.jsimpledb.MapValueIndexEntry} as appropriate. For example:
 * <pre>
 * public class <b>Company</b> {
 *
 *     &#64;JMapField(storageId = 11,
 *       key = &#64;JSimpleField(storageId = 12, <b>indexed = true</b>),
 *       value = &#64;JSimpleField(storageId = 13, type = "float")) // primitive type to disallow nulls
 *     public abstract Map&lt;<b>Vendor</b>, <b>Float</b>&gt; getAccountsPayable();
 *
 *     // Query to get mapping by Vendor of which Company's owe that vendor and how much they owe
 *     <b>&#64;IndexQuery("accountsPayable.key")</b>
 *     private abstract NavigableMap&lt;<b>Vendor</b>, NavigableSet&lt;<b>MapKeyIndexEntry&lt;Company, Float&gt;</b>&gt;&gt; queryVendorDebts();
 * }
 * </pre>
 * </p>
 *
 * <p>
 * The annotation {@link #value} contains the name of the indexed field to query (also known as a relative
 * {@link org.jsimpledb.ReferencePath} with zero intermediate references).
 * The field is assumed to be contained in the class containing the annotated method; however, a different
 * type may be specified via {@link #type}.
 * </p>
 *
 * <p>
 * The annotated method must be an instance method. Static methods would be more appropriate (the index query is
 * not specific to any instance), but static methods cannot be overridden by generated subclasses. To query an index
 * from a static context, see {@link org.jsimpledb.JTransaction#queryIndex JTransaction.queryIndex()} which provides
 * the same functionality as this annotation via direct method invocation.
 * </p>
 *
 * @see org.jsimpledb.JTransaction#queryIndex JTransaction.queryIndex()
 * @see org.jsimpledb.JTransaction#queryListFieldEntries(Class, String, Class) JTransaction.queryListFieldEntries()
 * @see org.jsimpledb.JTransaction#queryMapFieldKeyEntries(Class, String, Class, Class) JTransaction.queryMapFieldKeyEntries()
 * @see org.jsimpledb.JTransaction#queryMapFieldKeyEntries(Class, String, Class, Class) JTransaction.queryMapFieldKeyEntries()
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface IndexQuery {

    /**
     * Specifies the indexed field to query.
     *
     * <p>
     * For complex fields, the sub-field name must be included. For example, {@code "mylist.element"} and
     * {@code "mymap.value"} would be valid, but {@code "mymap"} would be invalid.
     * <p>
     *
     * <p>
     * In other words, the value must be a {@linkplain org.jsimpledb.ReferencePath reference path} with
     * zero references not ending on a complex field; see {@link org.jsimpledb.ReferencePath} for information
     * on the overall syntax for reference paths.
     * </p>
     *
     * @see org.jsimpledb.ReferencePath
     */
    String value();

    /**
     * Specifies the Java type containing the field specified by {@code #value}.
     * This may also be any super-type (e.g., an interface type), as long as the specified field is not ambiguous
     * among all sub-types.
     *
     * <p>
     * If this property is left unset, then then class containing the annotated method is assumed.
     * </p>
     *
     * @see org.jsimpledb.ReferencePath
     */
    Class<?> type() default void.class;
}

