
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;

import java.io.IOException;

import javax.xml.transform.Result;

import org.dellroad.stuff.java.IdGenerator;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.NestedRuntimeException;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.jibx.JibxMarshaller;

/**
 * Wrapper for Spring's {@link JibxMarshaller} that performs marshalling operations
 * within an invocation of {@link IdGenerator#run IdGenerator.run()}. Simply set your
 * normal {@link JibxMarshaller} to the {@linkplain #setJibxMarshaller jibxMarshaller}
 * property and use this class in its place.
 *
 * <p>
 * This is required when marshalling with JiBX mappings that utilize {@link IdMapper}.
 *
 * @see IdMapper
 */
public class IdMappingMarshaller implements Marshaller, InitializingBean {

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
     * {@link JibxMarshaller} within an invocation of {@link IdGenerator#run}.
     */
    @Override
    public void marshal(final Object graph, final Result result) throws IOException {
        try {
            IdGenerator.run(new Runnable() {
                @Override
                public void run() {
                    try {
                        IdMappingMarshaller.this.jibxMarshaller.marshal(graph, result);
                    } catch (IOException e) {
                        throw new NestedMarshalIOException(e);
                    }
                }
            });
        } catch (NestedMarshalIOException e) {
            throw e.getIOException();
        }
    }

    @Override
    public boolean supports(Class<?> type) {
        return this.jibxMarshaller.supports(type);
    }

    // Wrapper class for checked IOException
    @SuppressWarnings("serial")
    private static class NestedMarshalIOException extends NestedRuntimeException {

        NestedMarshalIOException(IOException e) {
            super(null, e);
        }

        public IOException getIOException() {
            return (IOException)this.getCause();
        }
    }
}

