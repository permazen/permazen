
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.IOException;
import java.io.InputStream;

/**
 * Callback interface used to read input from an {@link InputStream}.
 */
public interface ReadCallback {

    /**
     * Read from the given input stream.
     *
     * @param input input stream
     * @throws IOException if an I/O error occurs
     */
    void readFrom(InputStream input) throws IOException;
}

