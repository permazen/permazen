
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.IOException;

/**
 * Thrown by {@link AsyncOutputStream} when its buffer overflows.
 */
@SuppressWarnings("serial")
public class BufferOverflowException extends IOException {

    public BufferOverflowException() {
    }

    public BufferOverflowException(String message) {
        super(message);
    }
}

