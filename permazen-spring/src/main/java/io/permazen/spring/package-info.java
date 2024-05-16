
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

/**
 * Spring Framework integration classes.
 *
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/prism.min.js"></script>
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/components/prism-xml-doc.min.js"></script>
 * <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/themes/prism.min.css" rel="stylesheet"/>
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
 *      {@link io.permazen.Permazen} transactions.</li>
 *  <li>A {@link org.springframework.dao.support.PersistenceExceptionTranslator}
 *      {@linkplain io.permazen.spring.PermazenExceptionTranslator implementation} suitable for use with Permazen</li>
 *  <li>Various {@link org.springframework.http.converter.HttpMessageConverter}'s that bring Permazen's encoding,
 *      indexing, and schema management features to data being sent over a network:
 *      <ul>
 *      <li>{@link io.permazen.spring.KVStoreHttpMessageConverter} for encoding/decoding a raw
 *          {@link io.permazen.kv.KVStore}</li>
 *      <li>{@link io.permazen.spring.DetachedPermazenTransactionHttpMessageConverter} for encoding/decoding an entire
 *          {@link io.permazen.DetachedPermazenTransaction}</li>
 *      <li>{@link io.permazen.spring.PermazenObjectHttpMessageConverter} for encoding/decoding a specific
 *          {@link io.permazen.PermazenObject} within a {@link io.permazen.DetachedPermazenTransaction}</li>
 *      </ul>
 *  </li>
 * </ul>
 *
 * <p><b>Permazen XML Tags</b></p>
 *
 * <p>
 * Permazen makes available the following XML tags to Spring declarative XML. All elements live in the
 * {@code http://permazen.io/schema/spring/permazen} XML namespace:
 *
 * <div style="margin-left: 20px;">
 * <table class="striped">
 * <caption>Supported Elements</caption>
 * <tr style="bgcolor:#ccffcc">
 *  <th style="font-weight: bold; text-align: left">Element</th>
 *  <th style="font-weight: bold; text-align: left">Description</th>
 * </tr>
 * <tr>
 *  <td>{@code <permazen:scan-classes>}</td>
 *  <td>Works just like Spring's {@code <context:component-scan>} but finds
 *      {@link io.permazen.annotation.PermazenType &#64;PermazenType}-annotated Java model classes.
 *      Returns the classes found in a {@link java.util.List}.</td>
 * </tr>
 * <tr>
 *  <td>{@code <permazen:permazen>}</td>
 *  <td>Simplifies defining and configuring a {@link io.permazen.Permazen} database.</td>
 * </tr>
 * </table>
 * </div>
 *
 * <p>
 * The {@code <permazen:permazen>} element requires a nested {@code <permazen:scan-classes>} element to configure
 * the Java model classes. The {@code <permazen:permazen>} element supports the following attributes:
 *
 * <div style="margin-left: 20px;">
 * <table class="striped">
 * <caption>Supported Attributes</caption>
 * <tr style="bgcolor:#ccffcc">
 *  <th style="font-weight: bold; text-align: left">Attribute</th>
 *  <th style="font-weight: bold; text-align: left">Type</th>
 *  <th style="font-weight: bold; text-align: left">Required?</th>
 *  <th style="font-weight: bold; text-align: left">Description</th>
 * </tr>
 * <tr>
 *  <td>{@code kvstore}</td>
 *  <td>Bean reference</td>
 *  <td>No</td>
 *  <td>The underlying key/value store database. This should be the name of a Spring bean that implements
 *      the {@link io.permazen.kv.KVDatabase} interface. If unset, defaults to a new
 *      {@link io.permazen.kv.simple.SimpleKVDatabase} instance.</td>
 * </tr>
 * </table>
 * </div>
 *
 * <p>
 * An example Spring XML configuration using an in-memory key/value store and supporting Spring's
 * {@link org.springframework.transaction.annotation.Transactional &#64;Transactional} annotation:
 * <pre><code class="language-xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *   <b>xmlns:permazen="http://permazen.io/schema/spring/permazen"</b>
 *   xmlns:tx="http://www.springframework.org/schema/tx"
 *   xmlns:p="http://www.springframework.org/schema/p"
 *   xmlns:c="http://www.springframework.org/schema/c"
 *   xsi:schemaLocation="
 *      <b>http://permazen.io/schema/spring/permazen
 *        http://permazen.github.io/permazen/permazen-spring/src/main/resources/io/permazen/spring/permazen-1.1.xsd</b>
 *      http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *      http://www.springframework.org/schema/tx http://www.springframework.org/schema/tx/spring-tx.xsd"&gt;
 *
 *     &lt;!-- Define the underlying key/value database --&gt;
 *     &lt;bean id="kvdb" class="io.permazen.kv.simple.SimpleKVDatabase" p:waitTimeout="5000" p:holdTimeout="10000"/&gt;
 *
 *     &lt;!-- Define the Permazen database on top of that --&gt;
 *     &lt;<b>permazen:permazen</b> id="permazen" kvstore="kvdb"&gt;
 *
 *         &lt;!-- These are our Java model classes --&gt;
 *         &lt;<b>permazen:scan-classes</b> base-package="com.example.myapp"&gt;
 *             &lt;<b>permazen:exclude-filter</b> type="regex" expression="com\.example\.myapp\.test\..*"/&gt;
 *         &lt;/<b>permazen:scan-classes</b>&gt;
 *     &lt;/<b>permazen:permazen</b>&gt;
 *
 *     &lt;!-- Create a Permazen transaction manager --&gt;
 *     &lt;bean id="transactionManager" class="io.permazen.spring.PermazenTransactionManager"
 *       p:Permazen-ref="permazen"/&gt;
 *
 *     &lt;!-- Enable @Transactional annotations --&gt;
 *     &lt;tx:annotation-driven transaction-manager="transactionManager"/&gt;
 *
 * &lt;/beans&gt;
 * </code></pre>
 *
 * @see <a href="http://projects.spring.io/spring-framework/">Spring Framework</a>
 */
package io.permazen.spring;
