
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
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.util.XMLSerializer;

/**
 * Simple persistent {@link org.jsimpledb.kv.KVDatabase} backed by an XML file stored in a {@link StreamRepository}.
 * The data is kept in memory, and the XML file is rewritten in its entirety after each successful commit.
 * In normal usage, the XML file is stored in a regular {@link File} using a {@link FileStreamRepository}, which
 * guarantees (via the use of {@link AtomicUpdateFileOutputStream}) that a partially written XML file can never exist.
 *
 * <p>
 * If a {@link FileNotFoundException} is caught when trying to read the XML file, we assume that the underlying file has
 * not yet been created and the database will initially be empty.
 * </p>
 *
 * <p>
 * When a {@link FileStreamRepository} is used, instances support "out-of-band" updates of the XML file. In that case,
 * each time a transaction is accessed the modification timestamp of the XML file is examined. If the XML file has been
 * updated by some external process since the time the transaction was created, the database will be reloaded from
 * the XML file and the transaction will fail with a {@link RetryTransactionException}.
 * </p>
 *
 * <p>
 * Note that two different processes modifying the XML file at the same time is not without race conditions: e.g., it's possible
 * for an external process to update the XML file just as a transaction associated with this instance is being committed
 * and written to the file, which will result in overwriting the external process' changes.
 * </p>
 *
 * @see XMLSerializer
 * @see AtomicUpdateFileOutputStream
 */
public class XMLKVDatabase extends SimpleKVDatabase {

    private final StreamRepository repository;
    private final XMLSerializer serializer;
    private final File file;

    private int generation;
    private long timestamp;

    /**
     * Normal constructor. Uses a {@link FileStreamRepository} backed by the specified file.
     *
     * @param file persistent XML file
     * @throws IllegalArgumentException if {@code file} is null
     */
    public XMLKVDatabase(File file) {
        this(new FileStreamRepository(file));
    }

    /**
     * Constructor allowing storage in any user-supplied {@link StreamRepository}.
     *
     * @param repository XML file storage
     * @throws IllegalArgumentException if {@code file} is null
     */
    public XMLKVDatabase(StreamRepository repository) {
        if (repository == null)
            throw new IllegalArgumentException("null repository");
        this.repository = repository;
        this.serializer = new XMLSerializer(this.kv);
        this.reload();
        this.file = repository instanceof FileStreamRepository ? ((FileStreamRepository)repository).getFile() : null;
    }

    @Override
    public synchronized XMLKVTransaction createTransaction() {
        this.checkForOutOfBandUpdate();
        return new XMLKVTransaction(this, this.getWaitTimeout(), this.generation);
    }

    /**
     * Forcibly reload this database by re-reading the XML file.
     *
     * <p>
     * Any transactions that are in-progress when this method is called immediately become unusable.
     * </p>
     */
    public synchronized void reload() {
        this.readXML();
    }

    /**
     * Get the generation number associated with the XML file.
     * The generation number is incremented every time the database is wholesale updated by reading the file into memory,
     * e.g., by invoking {@link #reload}.
     *
     * @see XMLKVTransaction#getGeneration
     */
    public synchronized int getGeneration() {
        return this.generation;
    }

    /**
     * Check the XML file's timestamp and reload it if it has been modified since the most recent
     * read or write by this instance.
     *
     * @return true if file was updated and re-read, otherwise false
     */
    public synchronized boolean checkForOutOfBandUpdate() {
        if (this.file == null)
            return false;
        final long fileTime = this.file.lastModified();
        if (fileTime == 0)
            return false;
        if (this.timestamp != 0) {
            if (fileTime <= this.timestamp)
                return false;
            this.log.info("detected out-of-band update of XMLKVDatabase file `" + this.file + "'; reloading");
        }
        this.readXML();
        return true;
    }

    @Override
    protected void checkState(SimpleKVTransaction tx) {
        this.checkForOutOfBandUpdate();
        final int txGeneration = ((XMLKVTransaction)tx).getGeneration();
        if (txGeneration != this.generation) {
            throw new RetryTransactionException(tx, "XML file changed since transaction started (generation number changed from "
              + txGeneration + " to " + this.generation + ")");
        }
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

    protected synchronized void readXML() {
        this.generation++;
        this.kv.removeRange(null, null);
        if (this.file != null)
            this.timestamp = this.file.lastModified();
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

    protected synchronized void writeXML() {
        boolean successful = false;
        try {
            final OutputStream output = this.repository.getOutputStream();
            try {
                this.serializer.write(output, true);
                if (output instanceof AtomicUpdateFileOutputStream)
                    ((AtomicUpdateFileOutputStream)output).getFD().sync();
                output.close();
                if (this.file != null)
                    this.timestamp = this.file.lastModified();
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

