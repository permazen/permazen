
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.ant;

import org.apache.tools.ant.types.FileSet;

/**
 * Used by {@link SchemaGeneratorTask}.
 */
public class OldSchemas extends FileSet {

    public OldSchemas() {
    }

    protected OldSchemas(FileSet fileSet) {
        super(fileSet);
    }
}

