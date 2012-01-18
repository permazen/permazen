
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
import org.springframework.beans.factory.DisposableBean;
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
 *      &lt;!-- Persistent object delegate; you supply the XML (un)marshaller --&gt;
 *      <b>&lt;bean id="delegate" class="org.dellroad.stuff.pobj.SpringDelegate"
 *          p:marshaller-ref="marshaller" p:unmarshaller-ref="unmarshaller"/&gt;</b>
 *
 *      &lt;!-- Persistent object schema updater --&gt;
 *      <b>&lt;bean id="schemaUpdater" class="org.dellroad.stuff.pobj.SpringPersistentObjectSchemaUpdater"
 *          p:file="/var/example/pobj.xml" p:delegate-ref="delegate"
 *          p:initialXML="classpath:com/example/initial-pobj.xml"/&gt;</b>
 *
 *      &lt;!-- Persistent object bean --&gt;
 *      <b>&lt;bean scope="prototype" factory-bean="schemaUpdater" factory-method="getPersistentObject"/&gt;</b>
 *
 *      &lt;!-- Schema update #1 --&gt;
 *      <b>&lt;bean class="org.dellroad.stuff.pobj.SpringXSLPersistentObjectSchemaUpdate"
 *        id="update1" transform="classpath:com/example/updates/update1.xsl"/&gt;</b>
 *
 *      &lt;!-- Schema update #2 --&gt;
 *      <b>&lt;bean class="org.dellroad.stuff.pobj.SpringXSLPersistentObjectSchemaUpdate"
 *        id="update2" depends-on="update1" transform="classpath:com/example/updates/update2.xsl"/&gt;</b>
 *
 *      &lt;!-- Add more schema updates over time as needed and everything just works... --&gt;
 *
 *  &lt;/beans&gt;
 * </pre></blockquote>
 *
 * <p>
 * The {@link PersistentObject} itself, fully updated, is accessible via {@link #getPersistentObject}.
 * or, in the above example, via the <code>persistentObject</code> bean.
 *
 * @param <T> type of the root persistent object
 */
public class SpringPersistentObjectSchemaUpdater<T> extends PersistentObjectSchemaUpdater<T>
  implements BeanFactoryAware, InitializingBean, DisposableBean {

    private ListableBeanFactory beanFactory;
    private T initialValue;
    private Resource initialXML;

    /**
     * Set the initial value to be used on an uninitialized persistent object.
     *
     * <p>
     * Either this or a {@linkplain #setInitialXML initial XML resource} should be configured
     * to handle the case that no persistent file exists yet.
     */
    public void setInitialValue(T initialValue) {
        this.initialValue = initialValue;
    }

    /**
     * Set the resource containing the initial value, encoded as XML, to be used on an uninitialized persistent object.
     * This can be used as an alternative to {@link #setInitialValue}.
     *
     * <p>
     * Either this or an explicit {@linkplain #setInitialValue initial value} should be configured
     * to handle the case that no persistent file exists yet.
     */
    public void setInitialXML(Resource resource) {
        this.initialXML = resource;
    }

    /**
     * Get the initial value for the persistent object when no persistent file is found.
     *
     * <p>
     * The implementation in {@link SpringPersistentObjectSchemaUpdater} returns the {@linkplain #setInitialValue initial value},
     * if any, otherwise it falls back to decoding the initial value from the {@linkplain #setInitialXML initial value
     * resource}, if any. If neither property is configured, a {@link PersistentObjectException} is thrown.
     */
    @Override
    protected T getInitialValue() {

        // If value is provided explicitly, just return it
        if (this.initialValue != null)
            return this.initialValue;

        // Use configured XML if available
        if (this.initialXML == null)
            return null;
        try {
            InputStream input = this.initialXML.getInputStream();
            try {
                return this.delegate.deserialize(
                  new StreamSource(new BufferedInputStream(input), this.initialXML.getURI().toString()));
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

        // Check config
        if (this.file == null)
            throw new IllegalArgumentException("no file configured");
        if (this.writeDelay < 0)
            throw new IllegalArgumentException("negative writeDelay file configured");
        if (this.delegate == null)
            throw new IllegalArgumentException("no delegate configured");
        if (this.getUpdates() == null) {
            if (this.beanFactory == null) {
                throw new IllegalArgumentException("no updates explicitly configured and the containing BeanFactory"
                  + " is not a ListableBeanFactory: " + this.beanFactory);
            }
            this.setUpdates((Collection<SpringPersistentObjectSchemaUpdate<T>>)(Object)this.beanFactory.getBeansOfType(
              SpringPersistentObjectSchemaUpdate.class).values());
        }

        // Start
        this.start();
    }

    @Override
    public void destroy() {
        this.stop();
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

