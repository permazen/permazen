
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

/**
 * Spring Framework integration classes.
 *
 * <p><b>Overview</b></p>
 *
 * <p>
 * This package provides the following features to facilitate use with
 * <a href="http://projects.spring.io/spring-framework/">Spring</a>:
 * <ul>
 *  <li>Custom XML tags for Spring declarative XML (see below)</li>
 *  <li>A Spring {@link org.springframework.transaction.PlatformTransactionManager PlatformTransactionManager} that integrates
 *      into Spring's transaction infrastructure and enables the
 *      {@link org.springframework.transaction.annotation.Transactional &#64;Transactional} annotation for
 *      {@link org.jsimpledb.JSimpleDB} transactions.</li>
 *  <li>A {@link org.springframework.dao.support.PersistenceExceptionTranslator}
 *      {@linkplain org.jsimpledb.spring.JSimpleDBExceptionTranslator implementation} suitable for use with JSimpleDB</li>
 *  <li>{@link org.jsimpledb.spring.OpenTransactionInViewFilter}, which allows {@link org.jsimpledb.JSimpleDB}
 *      transactions to span an entire web request.</li>
 *  <li>Various {@link org.springframework.http.converter.HttpMessageConverter}'s that bring JSimpleDB's encoding,
 *      indexing, and schema management features to data being sent over a network:
 *      <ul>
 *      <li>{@link org.jsimpledb.spring.KVStoreHttpMessageConverter} for encoding/decoding a raw
 *          {@link org.jsimpledb.kv.KVStore}</li>
 *      <li>{@link org.jsimpledb.spring.SnapshotJTransactionHttpMessageConverter} for encoding/decoding an entire
 *          {@link org.jsimpledb.SnapshotJTransaction}</li>
 *      <li>{@link org.jsimpledb.spring.JObjectHttpMessageConverter} for encoding/decoding a specific
 *          {@link org.jsimpledb.JObject} within a {@link org.jsimpledb.SnapshotJTransaction}</li>
 *      </ul>
 *  </li>
 * </ul>
 *
 * <p><b>JSimpleDB XML Tags</b></p>
 *
 * <p>
 * JSimpleDB makes available the following XML tags to Spring declarative XML. All elements live in the
 * {@code http://jsimpledb.googlecode.com/schema/jsimpledb} XML namespace:
 *
 * <div style="margin-left: 20px;">
 * <table border="1" cellpadding="3" cellspacing="0" summary="Supported Elements">
 * <tr style="bgcolor:#ccffcc">
 *  <th align="left">Element</th>
 *  <th align="left">Description</th>
 * </tr>
 * <tr>
 *  <td>{@code <jsimpledb:scan-classes>}</td>
 *  <td>Works just like Spring's {@code <context:component-scan>} but finds
 *      {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}-annotated Java model classes.
 *      Returns the classes found in a {@link java.util.List}.</td>
 * </tr>
 * <tr>
 *  <td>{@code <jsimpledb:scan-field-types>}</td>
 *  <td>Works just like Spring's {@code <context:component-scan>} but finds
 *      {@link org.jsimpledb.annotation.JFieldType &#64;JFieldType}-annotated custom {@link org.jsimpledb.core.FieldType} classes.
 *      Returns the classes found in a {@link java.util.List}.</td>
 * </tr>
 * <tr>
 *  <td>{@code <jsimpledb:jsimpledb>}</td>
 *  <td>Simplifies defining and configuring a {@link org.jsimpledb.JSimpleDB} database.</td>
 * </tr>
 * </table>
 * </div>
 *
 * <p>
 * The {@code <jsimpledb:jsimpledb>} element requires a nested {@code <jsimpledb:scan-classes>} element to configure
 * the Java model classes. A nested {@code <jsimpledb:scan-field-types>} may also be included.
 * The {@code <jsimpledb:jsimpledb>} element supports the following attributes:
 *
 * <div style="margin-left: 20px;">
 * <table border="1" cellpadding="3" cellspacing="0" summary="Supported Attributes">
 * <tr style="bgcolor:#ccffcc">
 *  <th align="left">Attribute</th>
 *  <th align="left">Type</th>
 *  <th align="left">Required?</th>
 *  <th align="left">Description</th>
 * </tr>
 * <tr>
 *  <td>{@code kvstore}</td>
 *  <td>Bean reference</td>
 *  <td>No</td>
 *  <td>The underlying key/value store database. This should be the name of a Spring bean that implements
 *      the {@link org.jsimpledb.kv.KVDatabase} interface. If unset, defaults to a new
 *      {@link org.jsimpledb.kv.simple.SimpleKVDatabase} instance.</td>
 * </tr>
 * <tr>
 *  <td>{@code schema-version}</td>
 *  <td>Integer</td>
 *  <td>No</td>
 *  <td>The schema version corresponding to the configured Java model classes. A value of zero (the default)
 *      means to use whatever is the highest schema version already recorded in the database.</td>
 * </tr>
 * <tr>
 *  <td>{@code storage-id-generator}</td>
 *  <td>Bean reference</td>
 *  <td>No</td>
 *  <td>To use a custom {@link org.jsimpledb.StorageIdGenerator}, specify the name of a Spring bean that
 *      implements the {@link org.jsimpledb.StorageIdGenerator} interface. By default, a
 *      {@link org.jsimpledb.DefaultStorageIdGenerator} is used. If this attribute is set, then
 *      {@code auto-generate-storage-ids} must not be set to false.</td>
 * </tr>
 * <tr>
 *  <td>{@code auto-generate-storage-ids}</td>
 *  <td>Boolean</td>
 *  <td>No</td>
 *  <td>Whether to auto-generate storage ID's. Default is true</td>
 * </tr>
 * </table>
 * </div>
 *
 * <p>
 * An example Spring XML configuration using an in-memory key/value store and supporting Spring's
 * {@link org.springframework.transaction.annotation.Transactional &#64;Transactional} annotation:
 * <pre>
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *   <b>xmlns:jsimpledb="http://jsimpledb.googlecode.com/schema/jsimpledb"</b>
 *   xmlns:tx="http://www.springframework.org/schema/tx"
 *   xmlns:p="http://www.springframework.org/schema/p"
 *   xmlns:c="http://www.springframework.org/schema/c"
 *   xsi:schemaLocation="
 *      <b>http://jsimpledb.googlecode.com/schema/jsimpledb
 *        http://archiecobbs.github.io/jsimpledb/src/java/org/jsimpledb/spring/jsimpledb-1.0.xsd</b>
 *      http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *      http://www.springframework.org/schema/tx http://www.springframework.org/schema/tx/spring-tx.xsd"&gt;
 *
 *     &lt;!-- Define the underlying key/value database --&gt;
 *     &lt;bean id="kvdb" class="org.jsimpledb.kv.simple.SimpleKVDatabase" p:waitTimeout="5000" p:holdTimeout="10000"/&gt;
 *
 *     &lt;!-- Define the JSimpleDB database on top of that --&gt;
 *     &lt;<b>jsimpledb:jsimpledb</b> id="jsimpledb" kvstore="kvdb" schema-version="1"&gt;
 *
 *         &lt;!-- These are our Java model classes --&gt;
 *         &lt;<b>jsimpledb:scan-classes</b> base-package="com.example.myapp"&gt;
 *             &lt;<b>jsimpledb:exclude-filter</b> type="regex" expression="com\.example\.myapp\.test\..*"/&gt;
 *         &lt;/<b>jsimpledb:scan-classes</b>&gt;
 *
 *         &lt;!-- We have some custom FieldType's here too --&gt;
 *         &lt;<b>jsimpledb:scan-field-types</b> base-package="com.example.myapp.fieldtype"/&gt;
 *     &lt;/<b>jsimpledb:jsimpledb</b>&gt;
 *
 *     &lt;!-- Create a JSimpleDB transaction manager --&gt;
 *     &lt;bean id="transactionManager" class="org.jsimpledb.spring.JSimpleDBTransactionManager"
 *       p:JSimpleDB-ref="jsimpledb" p:allowNewSchema="true"/&gt;
 *
 *     &lt;!-- Enable @Transactional annotations --&gt;
 *     &lt;tx:annotation-driven transaction-manager="transactionManager"/&gt;
 *
 * &lt;/beans&gt;
 * </pre>
 *
 * @see <a href="http://projects.spring.io/spring-framework/">Spring Framework</a>
 */
package org.jsimpledb.spring;
