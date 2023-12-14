
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Thrown when an unknown index is accessed.
 */
@SuppressWarnings("serial")
public class UnknownIndexException extends DatabaseException {

    private final String indexName;

    /**
     * Constructor.
     *
     * @param indexName unknown index name
     * @param description description of the problem
     */
    public UnknownIndexException(String indexName, String description) {
        super(description);
        this.indexName = indexName;
    }

    /**
     * Get the name of the index that was not recognized.
     *
     * @return unrecognized index name
     */
    public String getIndexName() {
        return this.indexName;
    }
}
