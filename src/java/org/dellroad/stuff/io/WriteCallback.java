
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Callback interface used to write output to an {@link OutputStream}.
 */
public interface WriteCallback {

    /**
     * Write the output to the given output stream.
     *
     * @param output output stream
     * @throws IOException if an I/O error occurs
     */
    void writeTo(OutputStream output) throws IOException;
}

