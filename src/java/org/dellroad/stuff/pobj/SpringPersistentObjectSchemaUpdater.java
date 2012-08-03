
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Comparator;

import javax.xml.transform.stream.StreamSource;

import org.dellroad.stuff.schema.SchemaUpdate;
import org.dellroad.stuff.spring.BeanNameComparator;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.io.Resource;

/**
 * {@link PersistentObjectSchemaUpdater} optimized for use with Spring:
 * <ul>
 * <li>{@link #getOrderingTieBreaker} is overridden to break ties by ordering updates in the same order
 *  as they are defined in the bean factory.</li>
 * <li>This class implements {@link InitializingBean} and verifies all required properties are set.</li>
 * <li>If no updates are {@linkplain #setUpdates explicitly configured}, then all {@link SpringPersistentObjectSchemaUpdate}s
 *  found in the containing bean factory are automatically configured; this requires that all of the schema updates
 *  are defined in the same {@link ListableBeanFactory}.</li>
 * <li>The default value may be configured as an XML resource</li>
 * </ul>
 *
 * <p>
 * An example of how this class can be combined with custom XML to define an updater and all its updates:
 * <blockquote><pre>
 *  &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *    xmlns:p="http://www.springframework.org/schema/p"
 *    xsi:schemaLocation="
 *      http://www.springframework.org/schema/beans
 *        http://www.springframework.org/schema/beans/spring-beans-3.0.xsd"&gt;
 *
 *      &lt;!-- Our persistent object delegate. You supply the XML (un)marshaller (not shown). --&gt;
 *      <b>&lt;bean id="normalDelegate" class="org.dellroad.stuff.pobj.SpringDelegate"
 *        p:marshaller-ref="marshaller" p:unmarshaller-ref="unmarshaller"/&gt;</b>
 *
 *      &lt;!-- Schema updating persistent object delegate. The updates below will be auto-detected. --&gt;
 *      <b>&lt;bean id="updatingDelegate" class="org.dellroad.stuff.pobj.SpringPersistentObjectSchemaUpdater"
 *        p:marshaller-ref="marshaller" p:unmarshaller-ref="unmarshaller" p:defaultXML="classpath:default.xml"&gt;
 *          &lt;constructor-arg&gt;
 *              &lt;ref local="normalDelegate"/&gt;
 *          &lt;/constructor-arg&gt;
 *      &lt;/bean&gt;</b>
 *
 *      &lt;!-- Persistent object, configured to use our schema updating delegate --&gt;
 *      <b>&lt;bean id="persistentObject" class="org.dellroad.stuff.pobj.PersistentObject"
 *        init-method="start" destroy-method="stop" p:file="/var/lib/pobj.xml" p:allowEmptyStart="true"
 *        p:numBackups="3" p:delegate-ref="updatingDelegate"/&gt;</b>
 *
 *      &lt;!-- Define a default location for schema update XSL files --&gt;
 *      <b>&lt;bean class="org.dellroad.stuff.pobj.SpringXSLUpdateTransformConfigurer"
 *        p:prefix="classpath:updates/" p:suffix=".xsl"/&gt;</b>
 *
 *      &lt;!-- Schema update #1 with an explicitly configured XSL resource --&gt;
 *      <b>&lt;bean id="update1" class="org.dellroad.stuff.pobj.SpringXSLPersistentObjectSchemaUpdate"
 *        transform="file:///usr/share/updates/update1.xsl"/&gt;</b>
 *
 *      &lt;!-- Schema update #2: implicitly uses "classpath:updates/update2.xsl" --&gt;
 *      <b>&lt;bean id="update2" class="org.dellroad.stuff.pobj.SpringXSLPersistentObjectSchemaUpdate"/&gt;</b>
 *
 *      &lt;!-- Schema update #3: requires that update #1 be applied first --&gt;
 *      <b>&lt;bean id="update3" depends-on="update1"
 *        class="org.dellroad.stuff.pobj.SpringXSLPersistentObjectSchemaUpdate"/&gt;</b>
 *
 *      &lt;!-- Add more schema updates over time as needed and everything just works... --&gt;
 *
 *  &lt;/beans&gt;
 * </pre></blockquote>
 *
 * @param <T> type of the root persistent object
 */
public class SpringPersistentObjectSchemaUpdater<T> extends PersistentObjectSchemaUpdater<T>
  implements BeanFactoryAware, InitializingBean {

    private ListableBeanFactory beanFactory;
    private Resource defaultXML;

    /**
     * Constructor.
     *
     * @param delegate delegate that will be wrapped by this instance
     * @throws IllegalArgumentException if {@code delegate} is null
     * @see PersistentObjectSchemaUpdater#PersistentObjectSchemaUpdater
     */
    public SpringPersistentObjectSchemaUpdater(PersistentObjectDelegate<T> delegate) {
        super(delegate);
    }

    /**
     * Set the resource containing the default value, encoded as XML, to be used on an uninitialized persistent object.
     * This will override whatever default value is returned by the nested delegate.
     */
    public void setDefaultXML(Resource resource) {
        this.defaultXML = resource;
    }

    /**
     * Get the default value for the persistent object when no persistent file is found.
     *
     * <p>
     * The implementation in {@link SpringPersistentObjectSchemaUpdater} parses and returns the
     * {@linkplain #setDefaultXML default value resource}, if any; otherwise, the delegate provided
     * to the constructor is queried for a default value.
     */
    @Override
    public T getDefaultValue() {

        // If no XML configured, fall back to nested delegate
        if (this.defaultXML == null)
            return this.delegate.getDefaultValue();

        // Use configured XML
        try {
            this.log.info("loading default content from " + this.defaultXML.getURI());
            InputStream input = this.defaultXML.getInputStream();
            try {
                return this.delegate.deserialize(
                  new StreamSource(new BufferedInputStream(input), this.defaultXML.getURI().toString()));
            } finally {
                try {
                    input.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistentObjectException(e);
        }
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        if (beanFactory instanceof ListableBeanFactory)
            this.beanFactory = (ListableBeanFactory)beanFactory;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void afterPropertiesSet() throws Exception {
        if (this.getUpdates() == null) {
            if (this.beanFactory == null) {
                throw new IllegalArgumentException("no updates explicitly configured and the containing BeanFactory"
                  + " is not a ListableBeanFactory: " + this.beanFactory);
            }
            this.setUpdates((Collection<SpringPersistentObjectSchemaUpdate<T>>)(Object)this.beanFactory.getBeansOfType(
              SpringPersistentObjectSchemaUpdate.class).values());
        }
    }

    /**
     * Get the preferred ordering of two updates that do not have any predecessor constraints
     * (including implied indirect constraints) between them.
     *
     * <p>
     * In the case no schema updates are explicitly configured, the {@link Comparator} returned by the
     * implementation in {@link SpringPersistentObjectSchemaUpdater} sorts updates in the same order that they appear
     * in the containing {@link ListableBeanFactory}. Otherwise, the
     * {@linkplain org.dellroad.stuff.schema.AbstractSchemaUpdater#getOrderingTieBreaker superclass method} is used.
     */
    @Override
    protected Comparator<SchemaUpdate<PersistentFileTransaction>> getOrderingTieBreaker() {
        if (this.beanFactory == null)
            return super.getOrderingTieBreaker();
        final BeanNameComparator beanNameComparator = new BeanNameComparator(this.beanFactory);
        return new Comparator<SchemaUpdate<PersistentFileTransaction>>() {
            @Override
            public int compare(SchemaUpdate<PersistentFileTransaction> update1, SchemaUpdate<PersistentFileTransaction> update2) {
                return beanNameComparator.compare(update1.getName(), update2.getName());
            }
        };
    }
}

