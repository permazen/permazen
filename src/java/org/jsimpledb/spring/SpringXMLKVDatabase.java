
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.dellroad.stuff.io.StreamRepository;
import org.jsimpledb.kv.simple.XMLKVDatabase;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

/**
 * {@link XMLKVDatabase} that adds support for loading the default initial content from a Spring {@link Resource}.
 *
 * @see XMLKVDatabase
 */
public class SpringXMLKVDatabase extends XMLKVDatabase {

    private Resource initialContentResource;

// Constructors

    /**
     * Normal constructor. Uses a {@link org.dellroad.stuff.io.FileStreamRepository} backed by the specified file.
     *
     * @param file persistent XML file
     * @throws IllegalArgumentException if {@code file} is null
     */
    public SpringXMLKVDatabase(File file) {
        super(file);
    }

    /**
     * Constructor allowing storage in any user-supplied {@link StreamRepository}.
     *
     * @param repository XML file storage
     * @throws IllegalArgumentException if {@code file} is null
     */
    public SpringXMLKVDatabase(StreamRepository repository) {
        super(repository);
    }

    /**
     * Configure the {@link Resource} containing default initial content for an uninitialized database. This method is invoked
     * by {@link #getInitialContent} when, on the first load, the backing XML file is not found.
     *
     * @param initialContentResource resource containing default initial XML database content, or null for none
     */
    public void setInitialContentResource(Resource initialContentResource) {
        this.initialContentResource = initialContentResource;
    }

    /**
     * Configure the {@link Resource} containing default initial content for an uninitialized database from the specified file.
     *
     * @param initialContentFile file containing default initial XML database content, or null for none
     */
    @Override
    public void setInitialContentFile(File initialContentFile) {
        this.setInitialContentResource(initialContentFile != null ? new FileSystemResource(initialContentFile) : null);
    }

    /**
     * Get the initial content for an uninitialized database. This method is invoked when, on the first load,
     * the backing XML file is not found. It should return a stream that reads initial content for the database,
     * if any, otherwise null.
     *
     * <p>
     * The implementation in {@link SpringXMLKVDatabase} returns an {@link InputStream} acquired from the {@link Resource}
     * configured by {@link #setInitialContentResource setInitialContentResource()}, if any, otherwise null.
     * </p>
     *
     * @return default initial XML database content, or null for none
     */
    @Override
    protected InputStream getInitialContent() throws IOException {
        return this.initialContentResource != null ? this.initialContentResource.getInputStream() : null;
    }
}

