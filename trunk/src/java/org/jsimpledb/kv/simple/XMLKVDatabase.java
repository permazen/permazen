
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.simple;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import javax.xml.stream.XMLStreamException;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;
import org.dellroad.stuff.io.FileStreamRepository;
import org.dellroad.stuff.io.StreamRepository;
import org.jsimpledb.kv.KVDatabaseException;
import org.jsimpledb.kv.util.XMLSerializer;

/**
 * Simple persistent {@link org.jsimpledb.kv.KVDatabase} backed by an XML file stored in a {@link StreamRepository}.
 * The data is kept in memory, and the XML file is rewritten in its entirety after each successful commit.
 *
 * <p>
 * If a {@link FileNotFoundException} is caught when trying to read the XML file, we assume that the underlying file has
 * not yet been created and the database is initially empty.
 * </p>
 *
 * @see XMLSerializer
 * @see AtomicUpdateFileOutputStream
 */
public class XMLKVDatabase extends SimpleKVDatabase {

    private final StreamRepository repository;
    private final XMLSerializer serializer;

    /**
     * Constructor. Uses a {@link FileStreamRepository} backed by the specified file.
     *
     * @param file persistent XML file
     * @throws IllegalArgumentException if {@code file} is null
     */
    public XMLKVDatabase(File file) {
        this(new FileStreamRepository(file));
    }

    /**
     * Constructor.
     *
     * @param repository XML file storage
     * @throws IllegalArgumentException if {@code file} is null
     */
    public XMLKVDatabase(StreamRepository repository) {
        if (repository == null)
            throw new IllegalArgumentException("null repository");
        this.repository = repository;
        this.serializer = new XMLSerializer(this.kv);
        this.readXML();
    }

    @Override
    protected void postCommit(SimpleKVTransaction tx, boolean successful) {

        // If something weird happened, reload from storage
        if (!successful) {
            this.readXML();
            return;
        }

        // Persist data to file
        this.writeXML();
    }

    protected void readXML() {
        this.kv.removeRange(null, null);
        try {
            final BufferedInputStream input = new BufferedInputStream(this.repository.getInputStream());
            this.serializer.read(input);
        } catch (FileNotFoundException e) {
            // no problem, we'll create a new file
        } catch (IOException e) {
            throw new KVDatabaseException(this, "error reading XML content", e);
        } catch (XMLStreamException e) {
            throw new KVDatabaseException(this, "error reading XML content", e);
        }
    }

    protected void writeXML() {
        boolean successful = false;
        try {
            final OutputStream output = this.repository.getOutputStream();
            try {
                this.serializer.write(output, true);
                if (output instanceof AtomicUpdateFileOutputStream)
                    ((AtomicUpdateFileOutputStream)output).getFD().sync();
                output.close();
                successful = true;
            } finally {
                if (!successful && output instanceof AtomicUpdateFileOutputStream)
                    ((AtomicUpdateFileOutputStream)output).cancel();
            }
        } catch (IOException e) {
            throw new KVDatabaseException(this, "error writing XML content", e);
        } catch (XMLStreamException e) {
            throw new KVDatabaseException(this, "error writing XML content", e);
        }
    }
}

