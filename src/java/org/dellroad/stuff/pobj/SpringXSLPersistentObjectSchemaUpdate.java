
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;

import org.dellroad.stuff.xml.TransformErrorListener;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;

/**
 * {@link SpringPersistentObjectSchemaUpdate} that applies a configured XSL transform to the XML form of the persistent object.
 *
 * <p>
 * The {@link #setTransform transform} property is required.
 *
 * <p>
 * See {@link SpringPersistentObjectSchemaUpdater} for a Spring configuration example.
 *
 * @param <T> type of the persistent object
 */
public class SpringXSLPersistentObjectSchemaUpdate<T> extends SpringPersistentObjectSchemaUpdate<T> {

    private Resource transformResource;
    private Properties parameters;

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (this.transformResource == null)
            throw new Exception("no transform configured");
    }

    /**
     * Configure the XSLT transform as a resource.
     */
    public void setTransform(Resource transformResource) {
        this.transformResource = transformResource;
    }

    /**
     * Configure XSLT parameters. This is an optional property.
     */
    public void setParameters(Properties properties) {
        this.parameters = parameters;
    }

    /**
     * Apply this update to the given transaction.
     */
    @Override
    public void apply(PersistentFileTransaction transaction) {

        // Get transform source
        InputStream input;
        try {
            input = this.transformResource.getInputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {

            // Setup transformer
            Transformer transformer = TransformerFactory.newInstance().newTransformer(
              new StreamSource(input, this.transformResource.getURI().toString()));
            transformer.setErrorListener(new TransformErrorListener(LoggerFactory.getLogger(this.getClass()), true));
            if (this.parameters != null) {
                for (String name : this.parameters.stringPropertyNames())
                    transformer.setParameter(name, this.parameters.getProperty(name));
            }

            // Do the transform
            transaction.transform(transformer);

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TransformerException e) {
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof TransformerException && e.getCause().getCause() instanceof RuntimeException)
                e = (RuntimeException)e.getCause().getCause();
            throw e;
        } finally {
            try {
                input.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}

