
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import org.slf4j.LoggerFactory;

/**
 * Enumeration of possible actions to take when an error of some kind is detected.
 */
public enum ErrorAction {

    /**
     * Ignore the error: do nothing.
     */
    IGNORE {
        @Override
        public void execute(String message) {
        }
    },

    /**
     * Log the error.
     */
    LOG {
        @Override
        public void execute(String message) {
            LoggerFactory.getLogger(this.getClass()).error(message);
        }
    },

    /**
     * Raise an assertion error (if assertions are enabled).
     */
    ASSERT {
        @Override
        public void execute(String message) {
            assert false : message;
        }
    },

    /**
     * Throw a {@link RuntimeException}.
     */
    EXCEPTION {
        @Override
        public void execute(String message) {
            throw new RuntimeException(message);
        }
    };

    /**
     * Take the action appropriate for this instance.
     *
     * @param message a message describing the error
     * @throws AssertionError if this instance is {@link #ASSERT} and assertions are enabled
     * @throws RuntimeException if this instance is {@link #EXCEPTION}
     */
    public abstract void execute(String message);
}

