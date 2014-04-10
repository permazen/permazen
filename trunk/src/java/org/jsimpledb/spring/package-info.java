
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

/**
 * Spring Framework integration classes.
 *
 * <p>
 * This package provides the following features to facilitate use with Spring:
 * <ul>
 *  <li>The {@code <jsimpledb:jlayer-scan>} XML tag, which works just like Spring's {@code <context:component-scan>}
 *      to find {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass} and
 *      {@link org.jsimpledb.annotation.JFieldType &#64;JFieldType}-annotated classes.</li>
 *  <li>A Spring {@link org.springframework.transaction.PlatformTransactionManager PlatformTransactionManager} that integrates
 *      into Spring's transaction infrastructure and enables the
 *      {@link org.springframework.transaction.annotation.Transactional &#64;Transactional} annotation for
 *      {@link org.jsimpledb.jlayer.JLayer} transactions.</li>
 *  <li>{@link org.jsimpledb.spring.OpenTransactionInViewFilter}, which allows {@link org.jsimpledb.jlayer.JLayer}
 *      transactions to span an entire web request.</li>
 * </ul>
 * </p>
 *
 * <p>
 * An example Spring XML configuration that uses an in-memory key/value store:
 * <pre>
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *   <b>xmlns:jsimpledb="http://jsimpledb.googlecode.com/schema/jsimpledb"</b>
 *   xmlns:tx="http://www.springframework.org/schema/tx"
 *   xmlns:p="http://www.springframework.org/schema/p"
 *   xmlns:c="http://www.springframework.org/schema/c"
 *   xsi:schemaLocation="
 *      <b>http://jsimpledb.googlecode.com/schema/jsimpledb http://jsimpledb.googlecode.com/svn/schemas/jsimpledb-1.0.xsd</b>
 *      http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *      http://www.springframework.org/schema/tx http://www.springframework.org/schema/tx/spring-tx.xsd"&gt;
 *
 *     &lt;!-- Define the underlying key/value database --&gt;
 *     &lt;bean id="kvdb" class="org.jsimpledb.kv.simple.SimpleKVDatabase" p:waitTimeout="5000" p:holdTimeout="10000"/&gt;
 *
 *     &lt;!-- Define the core Database layer on top of that --&gt;
 *     &lt;bean id="jdb" class="org.jsimpledb.JSimpleDB" p:KVDatabase-ref="kvdb"/&gt;
 *
 *     &lt;!-- Define the Java "JLayer" on top of the JSimpleDB database --&gt;
 *     &lt;bean id="jlayer" class="org.jsimpledb.jlayer.JLayer" c:database-ref="jdb" c:version="1"&gt;
 *         &lt;constructor-arg&gt;
 *             &lt;<b>jsimpledb:jlayer-scan</b> base-package="com.example.myapp"&gt;
 *                 &lt;<b>jsimpledb:exclude-filter</b> type="regex" expression="com\.example\.myapp\.test\..*"/&gt;
 *             &lt;/<b>jsimpledb:jlayer-scan</b>&gt;
 *         &lt;/constructor-arg&gt;
 *     &lt;/bean&gt;
 *
 *     &lt;!-- Create a transaction manager --&gt;
 *     <b>&lt;bean id="transactionManager" class="org.jsimpledb.spring.JLayerTransactionManager" p:jlayer-ref="jlayer"/&gt;</b>
 *
 *     &lt;!-- Enable @Transactional annotations --&gt;
 *     <b>&lt;tx:annotation-driven transaction-manager="transactionManager"/&gt;</b>
 *
 * &lt;/beans&gt;
 * </pre>
 * </p>
 *
 * @see <a href="http://projects.spring.io/spring-framework/">Spring Framework</a>
 */
package org.jsimpledb.spring;
