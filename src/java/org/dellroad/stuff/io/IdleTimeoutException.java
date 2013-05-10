
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.IOException;

/**
 * Exception thrown by an {@link IdleTimeoutInputStream} when a read operation takes too long.
 */
@SuppressWarnings("serial")
public class IdleTimeoutException extends IOException {

    private final long timeout;

    public IdleTimeoutException(long timeout) {
        super("no input received after " + timeout + " milliseconds");
        this.timeout = timeout;
    }

    /**
     * Get the idle timeout value that was exceeded to cause this exception.
     *
     * @return idle timeout in milliseconds
     */
    public long getTimeout() {
        return this.timeout;
    }
}

