
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

/**
 * Permazen index classes. These classes define the Java interface to database indexes.
 *
 * <p>
 * Indexes provide fast lookup of objects based on field value(s). The {@code Index*} interfaces in this package have
 * generic type parameters that correspond to the field value type(s), plus a final generic type parameter
 * corresponding to the "target type". For example, an index on field {@code int }{@code getAccountNumber()} of type
 * {@code User} will be represented by a {@link io.permazen.index.Index}{@code <Integer, User>}, and may be viewed
 * either as a {@link java.util.NavigableSet}{@code <}{@link io.permazen.tuple.Tuple2}{@code <Integer, User>>}
 * or a {@link java.util.NavigableMap}{@code <Integer, }{@link java.util.NavigableSet}{@code <User>>}.
 *
 * <p>
 * Indexes are accessible through the {@link io.permazen.JTransaction} API:
 * <ul>
 *  <li>{@link io.permazen.JTransaction#queryIndex(Class, String, Class) JTransaction.queryIndex()}
 *      - Access the index associated with a simple field</li>
 *  <li>{@link io.permazen.JTransaction#queryListElementIndex JTransaction.queryListElementIndex()}
 *      - Access the composite index associated with a list field that includes corresponding list indices</li>
 *  <li>{@link io.permazen.JTransaction#queryMapValueIndex JTransaction.queryMapValueIndex()}
 *      - Access the composite index associated with a map value field that includes corresponding map keys</li>
 *  <li>{@link io.permazen.JTransaction#queryCompositeIndex(Class, String, Class, Class) JTransaction.queryCompositeIndex()}
 *      - Access a composite index defined on two fields</li>
 *  <li>{@link io.permazen.JTransaction#queryCompositeIndex(Class, String, Class, Class, Class)
 *      JTransaction.queryCompositeIndex()} - Access a composite index defined on three fields</li>
 *  <li>{@link io.permazen.JTransaction#queryCompositeIndex(Class, String, Class, Class, Class, Class)
 *      JTransaction.queryCompositeIndex()} - Access a composite index defined on four fields</li>
 *  <!-- COMPOSITE-INDEX -->
 *  <li>{@link io.permazen.JTransaction#queryVersion JTransaction.queryVersion()}
 *      - Get database objects grouped according to their schema versions</li>
 * </ul>
 *
 * <p>
 * <b>Simple and Composite Indexes</b>
 *
 * <p>
 * A simple index on a single field value is created by setting {@code indexed="true"} on the
 * {@link io.permazen.annotation.JField &#64;JField} annotation.
 *
 * <p>
 * Composite indexes on multiple fields are also supported. These are useful when the target type needs to be sorted
 * on multiple fields; for simple searching on multiple fields, it suffices to have independent, single-field indexes,
 * which can be intersected via {@link io.permazen.util.NavigableSets#intersection NavigableSets.intersection()}, etc.
 *
 * <p>
 * A composite index on two fields {@code String getUsername()} and {@code float getBalance()} of type {@code User}
 * will be represented by a {@link io.permazen.index.Index2}{@code <String, Float, User>}; a composite index on
 * three fields of type {@code X}, {@code Y}, and {@code Z} by a {@link io.permazen.index.Index3}{@code <X, Y, Z, User>}, etc.
 *
 * <p>
 * A composite index may be viewed as a set of tuples of indexed and target values, or as various mappings from one
 * or more indexed field values to subsequent values. A composite index may also be viewed as a simpler index on
 * any prefix of the indexed fields; for example, see {@link io.permazen.index.Index2#asIndex}.
 *
 * <p>
 * <b>Complex Sub-Fields</b>
 *
 * <p>
 * Only simple fields may be indexed, but the indexed field can be either a normal object field or a sub-field of
 * a complex {@link java.util.Set Set}, {@link java.util.List List}, or {@link java.util.Map Map} field. However,
 * complex sub-fields may not appear in composite indexes.
 *
 * <p>
 * For those complex sub-fields that can contain duplicate values (namely, {@link java.util.List} element and
 * {@link java.util.Map} value), the associated distinguishing value (respectively, {@link java.util.List} index
 * and {@link java.util.Map} key) is appended to the index and becomes the new target type.
 * Therefore the resulting index types associated with indexes on complex sub-fields are as follows:
 *
 * <div style="margin-left: 20px;">
 * <table class="striped">
 * <caption>Index Types</caption>
 * <tr style="bgcolor:#ccffcc">
 *  <th style="font-weight: bold; text-align: left">Complex Field</th>
 *  <th style="font-weight: bold; text-align: left">Indexed Sub-Field</th>
 *  <th style="font-weight: bold; text-align: left">Distinguising Value</th>
 *  <th style="font-weight: bold; text-align: left">Distinguising Type</th>
 *  <th style="font-weight: bold; text-align: left">Index Type</th>
 * </tr>
 * <tr>
 *  <td>{@link java.util.Set}{@code <E>}</td>
 *  <td>Set element</td>
 *  <td><i>n/a</i></td>
 *  <td><i>n/a</i></td>
 *  <td>{@link io.permazen.index.Index}{@code <E, Foobar>}</td>
 * </tr>
 * <tr>
 *  <td>{@link java.util.List}{@code <E>}</td>
 *  <td>List element</td>
 *  <td>List index</td>
 *  <td>{@link java.lang.Integer}</td>
 *  <td>{@link io.permazen.index.Index2}{@code <E, Foobar, Integer>}</td>
 * </tr>
 * <tr>
 *  <td>{@link java.util.Map}{@code <K, V>}</td>
 *  <td>Map key</td>
 *  <td><i>n/a</i></td>
 *  <td><i>n/a</i></td>
 *  <td>{@link io.permazen.index.Index}{@code <K, Foobar>}</td>
 * </tr>
 * <tr>
 *  <td>{@link java.util.Map}{@code <K, V>}</td>
 *  <td>Map value</td>
 *  <td>Map key</td>
 *  <td>{@code K}</td>
 *  <td>{@link io.permazen.index.Index2}{@code <V, Foobar, K>}</td>
 * </tr>
 * </table>
 * </div>
 */
package io.permazen.index;
