
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface for atomic storage and retrieval of an opaque byte array, with access being done
 * via {@link InputStream} and {@link OutputStream} interfaces.
 *
 * <p>
 * Conceptually, there is a single underlying byte array. Instances behave as if an atomic readable snapshot
 * is made when {@link #getInputStream} is invoked, and the results of that snapshot are then available
 * in the returned {@link InputStream}. Similarly, instances behave as if the underlying byte array is
 * atomically updated when the output stream returned by {@link #getOutputStream} is successfully
 * {@linkplain OutputStream#close closed}.
 *
 * <p>
 * Instances are thread safe, and support multiple concurrent open input and output streams. In particular,
 * two output streams may be {@linkplain OutputStream#close closed} at the same time by two different threads,
 * and one will always win the race.
 */
public interface StreamRepository {

    /**
     * Get an input stream reading the current value of the underlying store.
     *
     * @throws IOException if an error occurs
     */
    InputStream getInputStream() throws IOException;

    /**
     * Get an output stream writing to the underlying store. The underlying store is not affected until the
     * returned output stream is successfully {@linkplain OutputStream#close closed}, at which time it is
     * atomically updated with the newly written content.
     *
     * <p>
     * If the returned stread throws an {@link IOException} at any time, including during {@link OutputStream#close close()},
     * then no update to the underlying storage occurs.
     *
     * @throws IOException if an error occurs
     */
    OutputStream getOutputStream() throws IOException;
}

