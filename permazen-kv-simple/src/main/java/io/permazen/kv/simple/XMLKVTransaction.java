
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

/**
 * Transaction associated with a {@link XMLKVDatabase}.
 */
public class XMLKVTransaction extends SimpleKVTransaction {

    private final int generation;

    XMLKVTransaction(XMLKVDatabase database, long waitTimeout, int generation) {
        super(database, waitTimeout);
        this.generation = generation;
    }

    /**
     * Get the generation number associated with this instance.
     *
     * @return the generation number on which this transaction is based
     * @see XMLKVDatabase#getGeneration
     */
    public int getGeneration() {
        return this.generation;
    }
}

