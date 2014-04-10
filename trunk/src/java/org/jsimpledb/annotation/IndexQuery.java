
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
 * Annotates JLayer model class methods that query an indexed field.
 *
 * <p>
 * The annotated method must take zero parameters and return
 * {@link java.util.NavigableMap NavigableMap}{@code <V, }{@link java.util.NavigableSet NavigableSet}{@code <T>>}, where
 * {@code V} is the type of the indexed field's values, and {@code T} is the type of the Java model object
 * containing the field.
 * </p>
 *
 * <p>
 * For example:
 * <pre>
 * &#64;JSimpleClass(storageId = 10)
 * public class Person {
 *
 *     &#64;JSimpleSetField(storageId = 11, <b>indexed = true</b>)
 *     public abstract Set&lt;String&gt; getNicknames();
 *
 *     <b>&#64;IndexQuery("nicknames")</b>
 *     private abstract NavigableMap&lt;String, NavigableSet&lt;Person&gt;&gt; queryNicknames();
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
 * immediately.
 * </p>
 *
 * <p>
 * For list and map fields, you may also query the index for additional information beyond just which objects contain
 * the queried value in the indexed field: in the case of a list, you can also get the
 * list index, and in the case of a map, you can also get the corresponding key or value. For these kinds of query, specify the
 * type {@code T} to be either {@link org.jsimpledb.jlayer.ListIndexEntry},
 * {@link org.jsimpledb.jlayer.MapKeyIndexEntry}, or
 * {@link org.jsimpledb.jlayer.MapValueIndexEntry} as appropriate. For example:
 * <pre>
 * public class Company {
 *
 *     &#64;JSimpleMapField(storageId = 11,
 *       key = &#64;JSimpleField(storageId = 12, <b>indexed = true</b>),
 *       value = &#64;JSimpleField(storageId = 13, type = "float")) // primitive type to disallow nulls
 *     public abstract Map&lt;Vendor, Float&gt; getAccountsPayable();
 *
 *     // Query to get mapping by Vendor of which Company's owe that vendor and how much they owe
 *     <b>&#64;IndexQuery("accountsPayable.key")</b>
 *     private abstract NavigableMap&lt;Vendor, NavigableSet&lt;MapKeyIndexEntry&lt;Company, Float&gt;&gt;&gt; queryVendorDebts();
 * }
 * </pre>
 * </p>
 *
 * <p>
 * The annotation {@link #value} is a {@link org.jsimpledb.jlayer.ReferencePath} specifying the indexed
 * field to query. The path is assumed to start at the type containing the annotated method; however, a different
 * starting type may be specified via {@link #startType}.
 * </p>
 *
 * <p>
 * The annotated method must be an instance method. Note, static methods would be more appropriate (the index query is
 * not specific to any instance), but static methods cannot be overridden by generated subclasses. For this reason
 * it is sometimes useful to have a singleton Java model object type that only provides methods for querying indexes.
 * <p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface IndexQuery {

    /**
     * Specifies the target field to watch for changes.
     *
     * <p>
     * The value must be a {@linkplain org.jsimpledb.jlayer.ReferencePath reference path} with
     * zero references, e.g., {@code "MyClass.myfield"} or {@code "MyClass.mymap.key"}. The path cannot end on a
     * complex field itself; it must end on some specific sub-field. So for example {@code "MyClass.mymap"} would be invalid.
     * <p>
     *
     * <p>
     * See {@link org.jsimpledb.jlayer.ReferencePath} for information on the proper syntax for reference paths.
     * </p>
     *
     * @see org.jsimpledb.jlayer.ReferencePath
     */
    String value();

    /**
     * Specifies the starting type for the {@link org.jsimpledb.jlayer.ReferencePath} specified by {@code #value}.
     *
     * <p>
     * If this property is left unset, then then class containing the annotated method is assumed.
     * </p>
     *
     * @see org.jsimpledb.jlayer.ReferencePath
     */
    Class<?> startType() default void.class;
}

