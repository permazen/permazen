
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;

import java.io.IOException;
import java.util.concurrent.Callable;

import javax.xml.transform.Result;
import javax.xml.transform.Source;

import org.dellroad.stuff.java.IdGenerator;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.jibx.JibxMarshaller;

/**
 * Wrapper for Spring's {@link JibxMarshaller} that performs marshalling and unmarshalling operations
 * within an invocation of {@link IdGenerator#run IdGenerator.run()}. Simply set your
 * normal {@link JibxMarshaller} to the {@linkplain #setJibxMarshaller jibxMarshaller}
 * property and use this class in its place.
 *
 * <p>
 * This is required when marshalling with JiBX mappings that utilize {@link IdMapper}.
 *
 * @see IdMapper
 */
public class IdMappingMarshaller implements Marshaller, Unmarshaller, InitializingBean {

    private JibxMarshaller jibxMarshaller;

    /**
     * Configure the nested {@link JibxMarshaller}. Required property.
     */
    public void setJibxMarshaller(JibxMarshaller jibxMarshaller) {
        this.jibxMarshaller = jibxMarshaller;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.jibxMarshaller == null)
            throw new Exception("null jibxMarshaller");
    }

    /**
     * Invokdes {@link JibxMarshaller#marshal JibxMarshaller.marshal()} on the configured
     * {@link JibxMarshaller} within an invocation of {@link IdGenerator#run(Callable) IdGenerator.run()}.
     */
    @Override
    public void marshal(final Object graph, final Result result) throws IOException {
        try {
            IdGenerator.run(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    IdMappingMarshaller.this.jibxMarshaller.marshal(graph, result);
                    return null;
                }
            });
        } catch (Exception e) {
            this.unwrapException(e);
        }
    }

    /**
     * Invokdes {@link JibxMarshaller#unmarshal JibxMarshaller.unmarshal()} on the configured
     * {@link JibxMarshaller} within an invocation of {@link IdGenerator#run(Callable) IdGenerator.run()}.
     */
    @Override
    public Object unmarshal(final Source source) throws IOException {
        try {
            return IdGenerator.run(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return IdMappingMarshaller.this.jibxMarshaller.unmarshal(source);
                }
            });
        } catch (Exception e) {
            this.unwrapException(e);
            return null;                // never reached
        }
    }

    @Override
    public boolean supports(Class<?> type) {
        return this.jibxMarshaller.supports(type);
    }

    private void unwrapException(Exception e) throws IOException {
        if (e instanceof IOException)
            throw (IOException)e.getCause();
        if (e instanceof RuntimeException)
            throw (RuntimeException)e;
        throw new RuntimeException(e);
    }
}

