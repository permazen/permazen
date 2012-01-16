
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Comparator;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import org.dellroad.stuff.schema.SchemaUpdate;
import org.dellroad.stuff.spring.BeanNameComparator;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.io.Resource;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.Unmarshaller;

/**
 * {@link PersistentObjectSchemaUpdater} optimized for use with Spring.
 *
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
 * A Spring {@link Marshaller} and {@link Unmarshaller} are required for XML (de)serialization,
 * and an initial value (for when no persistent file exists) must be configured, specified either
 * explicitly or via an XML {@link Resource}.
 *
 * <p>
 * An example of how this class can be combined with custom XML to define an updater and all its updates:
 * <blockquote><pre>
 *  &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *    xmlns:p="http://www.springframework.org/schema/p"
 *    xsi:schemaLocation="
 *      http://www.springframework.org/schema/beans
 *        http://www.springframework.org/schema/beans/spring-beans-3.0.xsd&gt;
 *
 *      &lt;!-- Convenience definition to access the actual PersistentObject --&gt;
 *      <b>&lt;bean scope="prototype" factory-bean="schemaUpdater" factory-method="getPersistentObject"/&gt;</b>
 *
 *      &lt;!-- Persistent object schema updater; you supply the XML (un)marshallers --&gt;
 *      <b>&lt;bean id="schemaUpdater" class="org.dellroad.stuff.pobj.SpringPersistentObjectSchemaUpdater"
 *          p:marshaller-ref="marshaller" p:unmarshaller-ref="unmarshaller"
 *          p:file="/var/example/pobj.xml" p:initialXML="classpath:com/example/initial-pobj.xml"&gt;</b>
 *
 *      &lt;!-- Schema update #1 --&gt;
 *      <b>&lt;bean class="org.dellroad.stuff.pobj.SpringXSLPersistentObjectSchemaUpdate"
 *        id="update1" transform="classpath:com/example/updates/update1.xsl"&gt;</b>
 *
 *      &lt;!-- Schema update #2 --&gt;
 *      <b>&lt;bean class="org.dellroad.stuff.pobj.SpringXSLPersistentObjectSchemaUpdate"
 *        id="update2" depends-on="update1" transform="classpath:com/example/updates/update2.xsl"&gt;</b>
 *
 *      &lt;!-- Add more schema updates over time as needed and everything just works... --&gt;
 *
 *  &lt;/beans&gt;
 * </pre></blockquote>
 *
 * <p>
 * The {@link PersistentObject} itself, fully updated, is accessible via {@link #getPersistentObject}.
 *
 * @param <T> type of the root persistent object
 */
public class SpringPersistentObjectSchemaUpdater<T> extends PersistentObjectSchemaUpdater<T>
  implements BeanFactoryAware, InitializingBean, DisposableBean {

    private ListableBeanFactory beanFactory;
    private T initialValue;
    private Resource initialXML;
    private Marshaller marshaller;
    private Unmarshaller unmarshaller;

    /**
     * Set the {@link Marshaller} used to convert instances to XML. Required property.
     */
    public void setMarshaller(Marshaller marshaller) {
        this.marshaller = marshaller;
    }

    /**
     * Set the {@link Marshaller} used to convert instances to XML. Required property.
     */
    public void setUnmarshaller(Unmarshaller unmarshaller) {
        this.unmarshaller = unmarshaller;
    }

    /**
     * Set the initial value to be used on an uninitialized persistent object.
     *
     * <p>
     * Either this or a {@linkplain #setInitialXML initial XML resource} must be configured
     * in the case that no persistent file exists yet.
     */
    public void setInitialValue(T initialValue) {
        this.initialValue = initialValue;
    }

    /**
     * Set the resource containing the initial value, encoded as XML, to be used on an uninitialized persistent object.
     * This can be used as an alternative to {@link #setInitialValue}.
     *
     * <p>
     * Either this or an explicit {@linkplain #setInitialValue initial value} must be configured
     * in the case that no persistent file exists yet.
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

        // Use configured XML
        if (this.initialXML == null)
            throw new PersistentObjectException("an initial value is needed but no initial value (or resource) is configured");
        try {
            InputStream input = this.initialXML.getInputStream();
            try {
                return this.deserialize(new StreamSource(new BufferedInputStream(input), this.initialXML.getURI().toString()));
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
    public void serialize(T obj, Result result) {
        try {
            this.marshaller.marshal(obj, result);
        } catch (IOException e) {
            throw new PersistentObjectException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(Source source) {
        try {
            return (T)this.unmarshaller.unmarshal(source);
        } catch (IOException e) {
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
        if (this.beanFactory == null)
            throw new Exception("no bean factory configured");
        if (this.marshaller == null)
            throw new Exception("no marshaller configured");
        if (this.unmarshaller == null)
            throw new Exception("no unmarshaller configured");
        if (this.file == null)
            throw new Exception("no persistent file configured");
        if (this.getUpdates() == null) {
            if (this.beanFactory == null) {
                throw new IllegalArgumentException("no updates explicitly configured and the containing BeanFactory"
                  + " is not a ListableBeanFactory: " + this.beanFactory);
            }
            this.setUpdates((Collection<SpringPersistentObjectSchemaUpdate<T>>)(Object)this.beanFactory.getBeansOfType(
              SpringPersistentObjectSchemaUpdate.class).values());
        }
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

