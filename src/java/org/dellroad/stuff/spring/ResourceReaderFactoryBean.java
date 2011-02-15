
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.core.io.Resource;

/**
 * Spring factory bean that reads in a Spring {@link Resource} and converts it to a {@link String}.
 */
public class ResourceReaderFactoryBean extends AbstractFactoryBean<String> {

    private Resource resource;
    private String charset = "UTF-8";

    /**
     * Configure the resource containing the {@link String value}.
     */
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    /**
     * Configure the character encoding for the resource. Default is {@code UTF-8}.
     */
    public void setCharacterEncoding(String charset) {
        this.charset = charset;
    }

    @Override
    public Class<String> getObjectType() {
        return String.class;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (this.resource == null)
            throw new Exception("no resource configured");
    }

    @Override
    protected String createInstance() throws IOException {
        InputStream input = this.resource.getInputStream();
        try {
            InputStreamReader reader = new InputStreamReader(input, this.charset);
            StringWriter writer = new StringWriter();
            char[] buf = new char[4096];
            int r;
            while ((r = reader.read(buf)) != -1)
                writer.write(buf, 0, r);
            return writer.toString();
        } finally {
            try {
                input.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}

